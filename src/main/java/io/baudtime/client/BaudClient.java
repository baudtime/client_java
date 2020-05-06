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

import io.baudtime.client.netty.TcpClient;
import io.baudtime.message.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaudClient implements Client {

    private final TcpClient tcpClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    BaudClient(TcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public QueryResponse instantQuery(String queryExp, Date time, long timeout, TimeUnit unit) {
        String timeoutSec = String.valueOf(unit.toSeconds(timeout));

        InstantQueryRequest.Builder reqBuilder = InstantQueryRequest.newBuilder();
        reqBuilder.setQuery(queryExp).setTimeout(timeoutSec);
        if (time != null) {
            reqBuilder.setTime(time);
        }

        return (QueryResponse) tcpClient.query(reqBuilder.build(), timeout, unit);
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

        return (QueryResponse) tcpClient.query(reqBuilder.build(), timeout, unit);
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

        return (SeriesLabelsResponse) tcpClient.query(reqBuilder.build(), timeout, unit);
    }

    @Override
    public LabelValuesResponse labelValues(String name, Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        if (name == null) {
            throw new RuntimeException("label name must be provided");
        }

        String timeoutSec = String.valueOf(unit.toSeconds(timeout));

        LabelValuesRequest.Builder reqBuilder = LabelValuesRequest.newBuilder();
        reqBuilder.setName(name).setMatches(matches).setStart(start).setEnd(end).setTimeout(timeoutSec);

        return (LabelValuesResponse) tcpClient.query(reqBuilder.build(), timeout, unit);
    }

    @Override
    public void write(Series... series) {
        write(Arrays.asList(series));
    }

    @Override
    public void write(Collection<Series> series) {
        if (series == null || series.size() <= 0) {
            throw new RuntimeException("some series should be provided");
        }
        tcpClient.append(series);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            tcpClient.close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }
}
