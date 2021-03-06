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
import java.util.concurrent.TimeUnit;

public interface Client {
    QueryResponse instantQuery(String queryExp, Date time, long timeout, TimeUnit unit);

    QueryResponse rangeQuery(String queryExp, Date start, Date end, long step, long timeout, TimeUnit unit);

    SeriesLabelsResponse seriesLabels(Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit);

    LabelValuesResponse labelValues(String name, Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit);

    void write(Series... series);

    void write(Collection<Series> series);

    void close();

    boolean isClosed();
}
