/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.baudtime.client.netty;

import io.baudtime.client.ClientConfig;
import io.baudtime.discovery.ServiceAddrProvider;
import io.baudtime.message.AddRequest;
import io.baudtime.message.BaudMessage;
import io.baudtime.message.Series;
import io.baudtime.util.ConcurrentReferenceHashMap;
import io.baudtime.util.Util;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RoundRobinClient implements TcpClient {

    protected final AtomicLong opaque = new AtomicLong(0);
    protected final ClientConfig clientConfig;
    protected final ServiceAddrProvider serviceAddrProvider;
    protected final ResponseHandler responseHandler;
    protected final FutureListener writeResponseHook;

    private final EventLoopGroup eventLoopGroup;
    private final ChannelPoolMap<String /* addr */, FixedChannelPool> poolMap;

    private final ConcurrentMap<ChannelId, FlowControlBarrier> barriers = new ConcurrentReferenceHashMap<ChannelId, FlowControlBarrier>();

    public RoundRobinClient(final ClientConfig clientConfig, ServiceAddrProvider serviceAddrProvider, final FutureListener writeResponseHook) {
        this.clientConfig = clientConfig;
        this.responseHandler = new ResponseHandler();
        this.writeResponseHook = new FutureListener() {
            @Override
            public void onFinished(Future f) {
                responseHandler.releaseFuture(f);
                if (writeResponseHook != null) {
                    writeResponseHook.onFinished(f);
                }
            }
        };

        final Class<? extends Channel> channelClass;
        if (Util.OS.isLinux()) {
            this.eventLoopGroup = new EpollEventLoopGroup();
            channelClass = EpollSocketChannel.class;
        } else {
            this.eventLoopGroup = new NioEventLoopGroup();
            channelClass = NioSocketChannel.class;
        }

        final GlobalTrafficShapingHandler trafficShapingHandler = (clientConfig.getReadFlowControlLimit() > 0 || clientConfig.getWriteFlowControlLimit() > 0) ?
                new GlobalTrafficShapingHandler(Executors.newScheduledThreadPool(3),
                        clientConfig.getWriteFlowControlLimit(), clientConfig.getReadFlowControlLimit(), 500) : null;
        if (trafficShapingHandler != null && clientConfig.getWriteFlowControlLimit() > 0) {
            trafficShapingHandler.setMaxGlobalWriteSize(clientConfig.getWriteFlowControlLimit() + clientConfig.getWriteFlowControlLimit() / 10);
        }

        final ChannelPoolHandler channelPoolHandler = new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) {
                final FlowControlBarrier barrier = new FlowControlBarrier();
                barriers.putIfAbsent(ch.id(), barrier);

                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast(
                        new ResponseDecoder(clientConfig.getMaxResponseFrameLength()),
                        new RequestEncoder(),
                        new IdleStateHandler(0, 0, clientConfig.getChannelMaxIdleTimeSeconds()) {
                            @Override
                            protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) {
                                ctx.close();
                            }
                        },
                        responseHandler);

                if (trafficShapingHandler != null) {
                    pipeline.addLast(trafficShapingHandler);
                }

                if (!clientConfig.isFlushChannelOnEachWrite()) {
                    pipeline.addLast(new FlushConsolidationHandler(256, true));
                }

                pipeline.addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
                        if (ctx.channel().isWritable()) {
                            barrier.open();
                        } else {
                            barrier.close();
                        }
                        ctx.fireChannelWritabilityChanged();
                    }

                    @Override
                    public void channelUnregistered(ChannelHandlerContext ctx) {
                        barriers.remove(ctx.channel().id());
                        ctx.fireChannelUnregistered();
                    }
                });
                barrier.open();
            }
        };

        this.poolMap = new AbstractChannelPoolMap<String, FixedChannelPool>() {
            @Override
            protected FixedChannelPool newPool(String key) {
                String[] s = key.split(":");
                if (s.length != 2) {
                    throw new RuntimeException("invalid format of addr");
                }

                Bootstrap bootstrap = new Bootstrap()
                        .group(eventLoopGroup)
                        .channel(channelClass)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, clientConfig.getConnectTimeoutMillis())
                        .option(ChannelOption.SO_SNDBUF, clientConfig.getSocketSndBufSize())
                        .option(ChannelOption.SO_RCVBUF, clientConfig.getSocketRcvBufSize())
                        .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(clientConfig.getWriteBufLowWaterMark(), clientConfig.getWriteBufHighWaterMark()))
                        .remoteAddress(s[0], Integer.parseInt(s[1]));

                return new FixedChannelPool(bootstrap, channelPoolHandler, ChannelHealthChecker.ACTIVE,
                        FixedChannelPool.AcquireTimeoutAction.FAIL, clientConfig.getConnectTimeoutMillis(),
                        clientConfig.getMaxConnectionsOnEachServer(), clientConfig.getMaxConnectionsOnEachServer());
            }
        };

        this.serviceAddrProvider = serviceAddrProvider;
        this.serviceAddrProvider.watch();
    }

    @Override
    public BaudMessage query(BaudMessage request, long timeout, TimeUnit unit) {
        String addr = serviceAddrProvider.getServiceAddr();
        if (addr == null) {
            throw new RuntimeException("no server was found");
        }

        Channel ch;
        try {
            ch = getChannel(addr);
        } catch (RuntimeException e) {
            serviceAddrProvider.serviceDown(addr);
            throw e;
        }

        ensureWritable(ch);

        Message tcpMsg = new Message(opaque.getAndIncrement(), request);

        Future f = new Future(tcpMsg);
        responseHandler.registerFuture(f);

        try {
            ch.writeAndFlush(tcpMsg).addListener(f);
            return f.await(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            responseHandler.releaseFuture(f);
            putChannel(addr, ch);
        }
    }

    @Override
    public void append(Collection<Series> series) {
        String addr = serviceAddrProvider.getServiceAddr();
        if (addr == null) {
            throw new RuntimeException("no server was found");
        }

        Channel ch;
        try {
            ch = getChannel(addr);
        } catch (RuntimeException e) {
            serviceAddrProvider.serviceDown(addr);
            throw e;
        }

        ensureWritable(ch);

        try {
            AddRequest.Builder reqBuilder = AddRequest.newBuilder();
            reqBuilder.addSeries(series);

            Message tcpMsg = new Message(opaque.getAndIncrement(), reqBuilder.build());

            Future f = new Future(tcpMsg).addListener(writeResponseHook);
            responseHandler.registerFuture(f);

            if (clientConfig.isFlushChannelOnEachWrite()) {
                ch.writeAndFlush(tcpMsg).addListener(f);
            } else {
                ch.write(tcpMsg);

                if (!ch.isWritable() && ch.isOpen()) {
                    ch.flush();
                }
            }
        } finally {
            putChannel(addr, ch);
        }
    }

    @Override
    public void close() {
        this.serviceAddrProvider.stopWatch();
        this.eventLoopGroup.shutdownGracefully();
    }

    protected Channel getChannel(String addr) {
        FixedChannelPool pool = poolMap.get(addr);
        if (pool != null) {
            try {
                return pool.acquire().get();
            } catch (Exception e) {
                String msg = String.format("can't fetch a conn from pool, addr:%s, thread:%d", addr, Thread.currentThread().getId());
                throw new RuntimeException(msg, e);
            }
        }
        throw new RuntimeException("can't build conn pool for " + addr);
    }

    protected void putChannel(String addr, Channel channel) {
        FixedChannelPool pool = poolMap.get(addr);
        if (pool != null) {
            pool.release(channel).awaitUninterruptibly();
        }
    }

    protected void ensureWritable(Channel channel) {
        if (channel == null) {
            return;
        }
        FlowControlBarrier barrier = barriers.get(channel.id());
        if (barrier != null) {
            try {
                barrier.await();
            } catch (InterruptedException e) {
                throw new RuntimeException("flow control barrier is interrupted", e);
            }
        }
    }
}