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
import io.baudtime.discovery.ServiceAddrObserver;
import io.baudtime.discovery.ServiceAddrProvider;
import io.baudtime.message.AddRequest;
import io.baudtime.message.Series;
import io.baudtime.util.BaudtimeThreadFactory;
import io.baudtime.util.Util;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class StickyClient extends RoundRobinClient implements TcpClient {

    private final ThreadPoolExecutor workerThreads;
    private final List<Worker> workers = new ArrayList<Worker>();
    private static final AttributeKey<String> addrKey = AttributeKey.valueOf("addr");

    public StickyClient(ClientConfig clientConfig, ServiceAddrProvider serviceAddrProvider, FutureListener writeHook) {
        super(clientConfig, serviceAddrProvider, writeHook);

        ClientConfig.StickyConfig stickyConfig = clientConfig.getStickyConfig();
        int workNum = stickyConfig.getWorkerNum();

        workerThreads = new ThreadPoolExecutor(workNum, workNum, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new BaudtimeThreadFactory("nettyWorker"));
        for (int i = 0; i < workNum; i++) {
            Worker worker = new Worker(stickyConfig.getBatchSize(), stickyConfig.getQueueCapacity());
            workers.add(worker);
            workerThreads.submit(worker);
            serviceAddrProvider.addObserver(worker);
        }
    }

    @Override
    public void append(Collection<Series> series) {
        Map<Integer, AddRequest.Builder> builders = new HashMap<Integer, AddRequest.Builder>();

        for (Series s : series) {
            int idx = workerIndex(s.hash());

            AddRequest.Builder builder = builders.get(idx);
            if (builder == null) {
                builder = AddRequest.newBuilder();
                builders.put(idx, builder);
            }

            builder.addSeries(s);
        }

        Worker worker;
        for (Map.Entry<Integer, AddRequest.Builder> e : builders.entrySet()) {
            worker = getWorker(e.getKey());
            if (worker != null) {
                worker.submit(e.getValue());
            }
        }
    }

    @Override
    public void close() {
        for (Worker worker : workers) {
            worker.exit();
        }
        this.workerThreads.shutdown();
        super.close();
    }

    private int workerIndex(int hash) {
        return (hash & 0x0FFFFF) % workers.size();
    }

    private Worker getWorker(int index) {
        return workers.get(index);
    }

    private class Worker implements Runnable, ServiceAddrObserver {
        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private long backOff = 1;

        private final BlockingQueue<AddRequest.Builder> queue;
        private final int batchSize;

        private Channel ch;
        private AtomicBoolean shouldUpdate = new AtomicBoolean(false);

        private volatile boolean running = true;

        private Worker(int batchSize, int queueCapacity) {
            this.queue = new ArrayBlockingQueue<AddRequest.Builder>(queueCapacity);
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            while (running) {
                AddRequest.MergedBuilder merger = new AddRequest.MergedBuilder();

                while (merger.size() < batchSize) {
                    try {
                        AddRequest.Builder builder = this.queue.poll(200, TimeUnit.MILLISECONDS);
                        if (builder != null) {
                            merger.merge(builder);
                        } else {
                            break;
                        }
                    } catch (InterruptedException e) {
                        break;
                    }
                }

                try {
                    if (ch == null || !ch.isActive() || shouldUpdate.compareAndSet(true, false)) {
                        updateChannel();
                    }

                    if (ch != null && ch.isActive() && merger.size() > 0) {
                        ensureWritable(ch);

                        Message tcpMsg = new Message(opaque.getAndIncrement(), merger.build());

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
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }

            log.info("write worker exit");
        }

        private void submit(AddRequest.Builder addRequestBuilder) {
            if (!running) {
                return;
            }

            try {
                this.queue.put(addRequestBuilder);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void exit() {
            running = false;
        }

        private void updateChannel() throws InterruptedException {
            Thread.sleep(backOff);

            Channel oldCh = this.ch;
            this.ch = null;

            if (oldCh != null) {
                String oldAddr = oldCh.attr(addrKey).get();
                log.warn("switch channel from {} ...", oldAddr);

                oldCh.close();
                putChannel(oldAddr, oldCh);
            }

            String addr = serviceAddrProvider.getServiceAddr();
            if (addr == null) {
                throw new RuntimeException("no server was found");
            }

            Channel ch = null;
            try {
                ch = getChannel(addr);
            } catch (Exception e) {
                serviceAddrProvider.serviceDown(addr);
                log.error("failed to switch to " + addr, e);
            }

            if (ch == null) {
                backOff = Util.exponential(backOff, 1, 15000);
                return;
            } else {
                backOff = 1;
            }

            ch.attr(addrKey).set(addr);
            this.ch = ch;

            log.info("switched to {}", addr);
        }

        @Override
        public void addrChanged() {
            shouldUpdate.set(true);
        }

        @Override
        public void addrDown(String addr) {

        }

        @Override
        public void addrRecover(String addr) {
            shouldUpdate.set(true);
        }
    }

}
