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

import io.baudtime.message.LabelValuesResponse;
import io.baudtime.message.QueryResponse;
import io.baudtime.message.Series;
import io.baudtime.message.SeriesLabelsResponse;
import io.baudtime.util.BaudtimeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecordsAdaptor<Raw extends Client, Records> implements Client {
    public interface RecordsConverter<Records> {
        Collection<Series> convert(Records records);
    }

    private static final Logger log = LoggerFactory.getLogger(RecordsAdaptor.class);

    private ExecutorService convertExecutor;
    private final RecordsConverter<Records> recordsConverter;

    private ExecutorService senders = Executors.newCachedThreadPool(new BaudtimeThreadFactory("senders"));
    private static int senderNumPerQueue = 8;

    private List<WeakReference<BlockingQueue<Future<Collection<Series>>>>> reusefulQs = new ArrayList<WeakReference<BlockingQueue<Future<Collection<Series>>>>>(3);
    private static int reusefulQNum = 3;

    private final ThreadLocal<BlockingQueue<Future<Collection<Series>>>> futureQueue = new ThreadLocal<BlockingQueue<Future<Collection<Series>>>>();
    private final Raw client;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private RecordsAdaptor(final Raw client, RecordsConverter<Records> recordsConverter) {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        this.convertExecutor = new ThreadPoolExecutor(availableProcessors / 2, availableProcessors,
                10000L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(availableProcessors), new BaudtimeThreadFactory("recordProcessPre"),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        try {
                            executor.getQueue().put(r);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        this.recordsConverter = recordsConverter;
        this.client = client;
    }

    public static <Raw extends Client, Records> RecordsAdaptor<Raw, Records> wrap(Raw client, RecordsConverter<Records> recordsConverter) {
        return new RecordsAdaptor<Raw, Records>(client, recordsConverter);
    }

    public static <Raw extends Client, Records> RecordsAdaptor<Raw, Records> wrap(Raw client, RecordsConverter<Records> recordsConverter, int senderNumPerQueue) {
        RecordsAdaptor.senderNumPerQueue = senderNumPerQueue;
        return new RecordsAdaptor<Raw, Records>(client, recordsConverter);
    }

    @Override
    public QueryResponse instantQuery(String queryExp, Date time, long timeout, TimeUnit unit) {
        return client.instantQuery(queryExp, time, timeout, unit);
    }

    @Override
    public QueryResponse rangeQuery(String queryExp, Date start, Date end, long step, long timeout, TimeUnit unit) {
        return client.rangeQuery(queryExp, start, end, step, timeout, unit);
    }

    @Override
    public SeriesLabelsResponse seriesLabels(Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        return client.seriesLabels(matches, start, end, timeout, unit);
    }

    @Override
    public LabelValuesResponse labelValues(String name, String constraint, long timeout, TimeUnit unit) {
        return client.labelValues(name, constraint, timeout, unit);
    }

    @Override
    public void write(Series... series) {
        client.write(series);
    }

    @Override
    public void write(Collection<Series> series) {
        client.write(series);
    }

    public void write(final Records records) throws InterruptedException {
        if (client.isClosed()) {
            this.close();
            return;
        }
        Future<Collection<Series>> f = convertExecutor.submit(new Callable<Collection<Series>>() {
            @Override
            public Collection<Series> call() {
                return recordsConverter.convert(records);
            }
        });
        BlockingQueue<Future<Collection<Series>>> q = futureQueue.get();
        if (q == null) {
            q = getQueue();
            futureQueue.set(q);
        }
        q.put(f);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            convertExecutor.shutdownNow();
            senders.shutdownNow();
            client.close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    public Raw raw() {
        return client;
    }

    private synchronized BlockingQueue<Future<Collection<Series>>> getQueue() {
        BlockingQueue<Future<Collection<Series>>> q;

        if (reusefulQs.size() >= reusefulQNum) {
            Random r = new Random();
            WeakReference<BlockingQueue<Future<Collection<Series>>>> qRef = reusefulQs.get(r.nextInt(reusefulQs.size()));
            q = qRef.get();

            if (q != null) {
                return q;
            } else {
                reusefulQs.remove(qRef);
            }
        }

        q = new ArrayBlockingQueue<Future<Collection<Series>>>(96);
        final WeakReference<BlockingQueue<Future<Collection<Series>>>> qRef = new WeakReference<BlockingQueue<Future<Collection<Series>>>>(q);

        Runnable t = new Runnable() {
            @Override
            public void run() {
                Future<Collection<Series>> f;
                Collection<Series> series;

                while (!client.isClosed() && !closed.get()) {
                    if (qRef.get() == null) {
                        log.info("no threads reference this q any more");
                        return;
                    }
                    try {
                        f = qRef.get().poll(3000, TimeUnit.MILLISECONDS);
                        if (f != null) {
                            series = f.get();
                            if (series != null && !series.isEmpty()) {
                                client.write(series);
                            } else {
                                log.error("conversion error, series is empty");
                            }
                        } else {
                            System.gc();
                        }
                    } catch (InterruptedException e) {
                        log.error("record adaptor is interrupted");
                    } catch (Exception e) {
                        log.error("can't get result from preprocess ", e);
                    }
                }

                RecordsAdaptor.this.close();
            }
        };

        for (int i = 0; i < senderNumPerQueue; i++) {
            senders.execute(t);
        }

        reusefulQs.add(qRef);

        return q;
    }
}
