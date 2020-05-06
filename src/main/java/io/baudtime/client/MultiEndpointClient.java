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

import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiEndpointClient implements Client {

    public static final RuntimeException haveNotSelectEndpoint = new RuntimeException("should select a endpoint first");
    public static final RuntimeException noSuchEndpoint = new RuntimeException("no such endpoint");

    public volatile Client current;

    public ConcurrentHashMap<String, Client> clients = new ConcurrentHashMap<String, Client>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    void addEndpoint(String endpoint, Client client) {
        clients.put(endpoint, client);
    }

    public Client use(String endpoint) {
        Client c = getClient(endpoint);
        current = c;
        return c;
    }

    private Client getClient(String endpoint) {
        Client c = clients.get(endpoint);
        if (c != null) {
            return c;
        }
        throw noSuchEndpoint;
    }

    @Override
    public QueryResponse instantQuery(String queryExp, Date time, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.instantQuery(queryExp, time, timeout, unit);
    }

    @Override
    public QueryResponse rangeQuery(String queryExp, Date start, Date end, long step, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.rangeQuery(queryExp, start, end, step, timeout, unit);
    }

    @Override
    public SeriesLabelsResponse seriesLabels(Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.seriesLabels(matches, start, end, timeout, unit);
    }

    @Override
    public LabelValuesResponse labelValues(String name, Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.labelValues(name, matches, start, end, timeout, unit);
    }

    @Override
    public void write(Series... series) {
        checkCurrentSelect();
        current.write(series);
    }

    @Override
    public void write(Collection<Series> series) {
        checkCurrentSelect();
        current.write(series);
    }

    public QueryResponse instantQuery(String endpoint, String queryExp, Date time, long timeout, TimeUnit unit) {
        return getClient(endpoint).instantQuery(queryExp, time, timeout, unit);
    }

    public QueryResponse rangeQuery(String endpoint, String queryExp, Date start, Date end, long step, long timeout, TimeUnit unit) {
        return getClient(endpoint).rangeQuery(queryExp, start, end, step, timeout, unit);
    }

    public SeriesLabelsResponse seriesLabels(String endpoint, Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        return getClient(endpoint).seriesLabels(matches, start, end, timeout, unit);
    }

    public LabelValuesResponse labelValues(String endpoint, String name, Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        return getClient(endpoint).labelValues(name, matches, start, end, timeout, unit);
    }

    public void write(String endpoint, Series... series) {
        getClient(endpoint).write(series);
    }

    public void write(String endpoint, Collection<Series> series) {
        getClient(endpoint).write(series);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (Client c : clients.values()) {
                c.close();
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    private void checkCurrentSelect() {
        if (current == null) {
            throw haveNotSelectEndpoint;
        }
    }
}
