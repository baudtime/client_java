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
import io.baudtime.message.Hashed;
import io.baudtime.message.Series;
import io.baudtime.util.BaudtimeThreadFactory;
import io.baudtime.util.Util;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class StickyClient extends RoundRobinClient implements TcpClient {

    private final ThreadPoolExecutor workerThreads;
    private final List<Worker> workers = new ArrayList<Worker>();

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
        Worker workerForBatch = null;
        if (series instanceof Hashed) {
            workerForBatch = getWorker((Hashed) series);
        }

        List<Series> tryAgain = new LinkedList<Series>();
        Worker worker;

        for (Series s : series) {
            worker = (workerForBatch != null ? workerForBatch : getWorker(s));
            if (worker != null) {
                try {
                    worker.submit(s, false);
                } catch (IllegalStateException e) {
                    tryAgain.add(s);
                }
            }
        }

        if (!tryAgain.isEmpty()) {
            for (Series s : tryAgain) {
                worker = (workerForBatch != null ? workerForBatch : getWorker(s));
                if (worker != null) {
                    worker.submit(s, true);
                }
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

    private Worker getWorker(Hashed hashed) {
        int idx = (hashed.hash() & 0x0FFFF) % workers.size();
        return workers.get(idx);
    }

    private class Worker implements Runnable, ServiceAddrObserver {
        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private long backOff = 1;

        private final BlockingQueue<Series> queue;
        private final int batchSize;

        private Channel ch;
        private String addr;
        private AtomicBoolean shouldUpdate = new AtomicBoolean(false);

        private volatile boolean running = true;

        private Worker(int batchSize, int queueCapacity) {
            this.queue = new ArrayBlockingQueue<Series>(queueCapacity);
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            while (running) {
                AddRequest.Builder reqBuilder = AddRequest.newBuilder();

                while (reqBuilder.size() < batchSize) {
                    try {
                        Series s = this.queue.poll(3, TimeUnit.MILLISECONDS);
                        if (s != null) {
                            reqBuilder.addSeries(s);
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

                    if (ch != null && ch.isActive() && reqBuilder.size() > 0) {
                        ensureWritable(ch);

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
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }

            log.info("write worker exit");
        }

        private void submit(Series series, boolean backPressure) {
            if (!running) {
                return;
            }

            if (backPressure) {
                try {
                    this.queue.put(series);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                this.queue.add(series);
            }
        }

        private void exit() {
            running = false;
        }

        private void updateChannel() throws InterruptedException {
            Thread.sleep(backOff);

            log.warn("switch channel...");

            Channel oldCh = this.ch;
            String oldAddr = this.addr;
            this.ch = null;
            this.addr = null;

            if (oldCh != null) {
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

            this.ch = ch;
            this.addr = addr;

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
