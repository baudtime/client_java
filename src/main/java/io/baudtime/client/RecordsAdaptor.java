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

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.*;

public class RecordsAdaptor<Raw extends Client, Records> implements Client {
    public interface RecordsConverter<Records> {
        Collection<Series> convert(Records records);
    }

    private static final Logger log = LoggerFactory.getLogger(RecordsAdaptor.class);

    private ExecutorService executor;
    private final BlockingQueue<Future<Collection<Series>>> futures = new ArrayBlockingQueue<Future<Collection<Series>>>(96);
    private final RecordsConverter<Records> recordsConverter;

    private final Raw client;

    private RecordsAdaptor(final Raw client, RecordsConverter<Records> recordsConverter) {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        this.executor = new ThreadPoolExecutor(availableProcessors / 2, availableProcessors,
                10000L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(availableProcessors), new BaudtimeThreadFactory("recordProcess"),
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

        executor.execute(new Runnable() {
            @Override
            public void run() {
                Future<Collection<Series>> f;
                Collection<Series> series;

                while (true) {
                    try {
                        f = futures.take();
                        series = f.get();
                        if (series != null && !series.isEmpty()) {
                            client.write(series);
                        } else {
                            log.error("conversion error, series is empty");
                        }
                    } catch (InterruptedException e) {
                        log.error("preprocess interrupted ", e);
                        return;
                    } catch (Exception e) {
                        log.error("can't get result from preprocess ", e);
                    }
                }
            }
        });
    }

    public static <Raw extends Client, Records> RecordsAdaptor<Raw, Records> wrap(Raw client, RecordsConverter<Records> recordsConverter) {
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
        Future<Collection<Series>> f = executor.submit(new Callable<Collection<Series>>() {
            @Override
            public Collection<Series> call() {
                return recordsConverter.convert(records);
            }
        });
        futures.put(f);
    }

    @Override
    public void close() {
        client.close();
    }

    public Raw raw() {
        return client;
    }
}
