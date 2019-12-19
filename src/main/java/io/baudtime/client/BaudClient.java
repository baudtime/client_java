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

package io.baudtime.client;

import io.baudtime.discovery.ServiceAddrProvider;
import io.baudtime.message.*;
import io.baudtime.util.Util;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class BaudClient implements Client {

    private static final Logger log = LoggerFactory.getLogger(BaudClient.class);

    private final AtomicLong opaque = new AtomicLong(0);

    private final ClientConfig clientConfig;
    private final ConcurrentMap<String /* addr */, Channel> channelTables = new ConcurrentHashMap<String, Channel>();
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroup;
    private final ResponseHandler responseHandler;
    private final ServiceAddrProvider serviceAddrProvider;

    BaudClient(final ClientConfig clientConfig, ServiceAddrProvider serviceAddrProvider, WriteResponseHook writeHook) {
        this.clientConfig = clientConfig;

        if (Util.OS.isLinux()) {
            this.eventLoopGroup = new EpollEventLoopGroup();
            this.bootstrap.group(this.eventLoopGroup)
                    .channel(EpollSocketChannel.class);
        } else {
            this.eventLoopGroup = new NioEventLoopGroup();
            this.bootstrap.group(this.eventLoopGroup)
                    .channel(NioSocketChannel.class);
        }

        this.bootstrap.option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, clientConfig.getConnectTimeoutMillis())
                .option(ChannelOption.SO_SNDBUF, clientConfig.getSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, clientConfig.getSocketRcvBufSize())
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(clientConfig.getWriteBufLowWaterMark(), clientConfig.getWriteBufHighWaterMark()))//before {16 * 1024, 24 * 1024}
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast(
                                new RequestEncoder(),
                                new ResponseDecoder(clientConfig.getMaxResponseFrameLength()),
//                                new IdleStateHandler(0, 0, clientConfig.getChannelMaxIdleTimeSeconds()),
                                responseHandler);
                        if (!clientConfig.isFlushChannelOnEachWrite()) {
                            pipeline.addFirst(new FlushConsolidationHandler(256, true));
                        }
                    }
                });

        this.serviceAddrProvider = serviceAddrProvider;
        this.responseHandler = new ResponseHandler(writeHook);
        this.serviceAddrProvider.watch();
    }

    @Override
    public QueryResponse instantQuery(String queryExp, Date time, long timeout, TimeUnit unit) {
        String timeoutSec = String.valueOf(unit.toSeconds(timeout));

        InstantQueryRequest.Builder reqBuilder = InstantQueryRequest.newBuilder();
        reqBuilder.setQuery(queryExp).setTimeout(timeoutSec);
        if (time != null) {
            reqBuilder.setTime(time);
        }

        return (QueryResponse) sendRequest(reqBuilder.build(), timeout, unit);
    }

    @Override
    public QueryResponse rangeQuery(String queryExp, Date start, Date end, long step, long timeout, TimeUnit unit) {
        if (start == null) {
            throw new RuntimeException("start time must be provided");
        }
        if (end == null) {
            throw new RuntimeException("end time must be provided");
        }

        if (end.before(start)) {
            throw new RuntimeException("end time must not be before start time");
        }

        if (step <= 0) {
            throw new RuntimeException("zero or negative query resolution step widths are not accepted. Try a positive integer");
        }

        if ((end.getTime() - start.getTime()) / unit.toMillis(step) > 11000) {
            throw new RuntimeException("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)");
        }

        String timeoutSec = String.valueOf(unit.toSeconds(timeout));
        String stepSec = String.valueOf(unit.toSeconds(step));

        RangeQueryRequest.Builder reqBuilder = RangeQueryRequest.newBuilder();
        reqBuilder.setQuery(queryExp).setTimeout(timeoutSec).setStart(start).setEnd(end).setStep(stepSec);

        return (QueryResponse) sendRequest(reqBuilder.build(), timeout, unit);
    }

    @Override
    public SeriesLabelsResponse seriesLabels(Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        if (start == null) {
            throw new RuntimeException("start time must be provided");
        }
        if (end == null) {
            throw new RuntimeException("end time must be provided");
        }

        if (end.before(start)) {
            throw new RuntimeException("end time must not be before start time");
        }

        String timeoutSec = String.valueOf(unit.toSeconds(timeout));

        SeriesLabelsRequest.Builder reqBuilder = SeriesLabelsRequest.newBuilder();
        reqBuilder.setMatches(matches).setStart(start).setEnd(end).setTimeout(timeoutSec);

        return (SeriesLabelsResponse) sendRequest(reqBuilder.build(), timeout, unit);
    }

    @Override
    public LabelValuesResponse labelValues(String name, String constraint, long timeout, TimeUnit unit) {
        if (name == null) {
            throw new RuntimeException("label name must be provided");
        }

        String timeoutSec = String.valueOf(unit.toSeconds(timeout));

        LabelValuesRequest.Builder reqBuilder = LabelValuesRequest.newBuilder();
        reqBuilder.setName(name).setTimeout(timeoutSec);
        if (constraint != null) {
            reqBuilder.setConstraint(constraint);
        }

        return (LabelValuesResponse) sendRequest(reqBuilder.build(), timeout, unit);
    }

    @Override
    public ResponseFuture write(Series... series) {
        return write(Arrays.asList(series));
    }

    @Override
    public ResponseFuture write(Collection<Series> series) {
        if (series == null || series.size() <= 0) {
            throw new RuntimeException("some series should be provided");
        }

        AddRequest.Builder reqBuilder = AddRequest.newBuilder();
        reqBuilder.addSeries(series);

        return this.sendRequest(reqBuilder.build());
    }

    @Override
    public void close() {
        this.serviceAddrProvider.stopWatch();
        for (Map.Entry<String, Channel> e : this.channelTables.entrySet()) {
            final String addr = e.getKey();
            Channel c = e.getValue();

            c.closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    log.info("closeChannel: close the connection to remote address[{}] result: {}", addr, future.isSuccess());
                }
            });
        }

        this.eventLoopGroup.shutdownGracefully();
    }

    private BaudMessage sendRequest(BaudMessage request, long timeout, TimeUnit unit) {
        String addr = serviceAddrProvider.getServiceAddr();
        if (addr == null) {
            throw new RuntimeException("no server was found");
        }

        Channel ch = getChannel(addr);
        if (ch == null) {
            throw new RuntimeException("can't connect to server");
        }

        Message tcpMsg = new Message(opaque.getAndIncrement(), request);

        ResponseFuture f = new ResponseFuture(tcpMsg.getOpaque());
        responseHandler.registerFuture(f);

        try {
            try {
                ch.writeAndFlush(tcpMsg);
            } catch (Exception e) {
                channelTables.remove(addr);
                ch.close();
                throw new RuntimeException(e);
            }

            try {
                return f.await(timeout, unit);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } finally {
            responseHandler.releaseFuture(f);
        }
    }

    private ResponseFuture sendRequest(BaudMessage request) {
        String addr = serviceAddrProvider.getServiceAddr();
        if (addr == null) {
            throw new RuntimeException("no server was found");
        }

        Channel ch = getChannel(addr);
        if (ch == null) {
            throw new RuntimeException("can't connect to server");
        }

        Message tcpMsg = new Message(opaque.getAndIncrement(), request);

        ResponseFuture f = new ResponseFuture(tcpMsg.getOpaque());
        responseHandler.registerFuture(f);

        try {
            if (this.clientConfig.isFlushChannelOnEachWrite()) {
                ch.writeAndFlush(tcpMsg);
            } else {
                ch.write(tcpMsg);

                if (!ch.isWritable() && ch.isOpen()) {
                    ch.flush();
                }
            }
        } catch (Exception e) {
            channelTables.remove(addr);
            ch.close();
            throw new RuntimeException(e);
        }

        return f;
    }

    private Channel getChannel(String addr) {
        Channel c = this.channelTables.get(addr);
        if (c != null && c.isActive()) {
            return c;
        }

        String[] s = addr.split(":");
        if (s.length != 2) {
            throw new RuntimeException("invalid format of addr");
        }

        ChannelFuture channelFuture = this.bootstrap.connect(s[0], Integer.parseInt(s[1]));
        if (channelFuture != null) {
            channelFuture.awaitUninterruptibly();
            Channel newc = channelFuture.channel();

            if (!channelFuture.isSuccess()) {
                newc.close();
                serviceAddrProvider.serviceDown(addr);
                log.error("connect to {} failed when get channel, err: {}", addr, channelFuture.cause());
            } else if (!newc.isActive()) {
                newc.close();
                log.info("channel not active when get channel, {}", addr);
            } else {
                synchronized (this.channelTables) {
                    Channel oldc = this.channelTables.put(addr, newc);
                    if (oldc != null) {
                        oldc.close();
                    }
                }
                return newc;
            }
        }

        return null;
    }
}
