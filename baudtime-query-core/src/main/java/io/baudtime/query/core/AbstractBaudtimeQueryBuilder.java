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

package io.baudtime.query.core;

import io.baudtime.client.Client;
import io.baudtime.message.QueryResponse;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;


public abstract class AbstractBaudtimeQueryBuilder<T extends AbstractBaudtimeQueryBuilder<T, Q, R>, Q extends BaudtimeQuery<R>, R> implements BaudtimeQueryBuilder<T, Q, R> {

    protected String metric;

    protected List<String> filters;

    protected String offset;

    protected BaudtimeOperator operator;

    protected long queryTimeoutSec = 30;

    protected String duration;

    protected BaudtimeQueryResponseConverter<QueryResponse, R> responseConverter;

    protected Client baudtimeClient;

    public AbstractBaudtimeQueryBuilder(BaudtimeQueryResponseConverter<QueryResponse, R> responseConverter) {
        this.responseConverter = responseConverter;
    }

    @Override
    public String queryExp() {

        StringBuilder queryExpBuilder = new StringBuilder();

        if (metric != null) {
            queryExpBuilder.append(metric);
        }

        if (filters != null && !filters.isEmpty()) {
            queryExpBuilder.append("{").append(String.join(",", filters)).append("}");
        }

        if (duration != null) {
            queryExpBuilder.append("[").append(duration).append("]");
        }

        if (offset != null) {
            queryExpBuilder.append(" offset ").append(offset);
        }

        if (operator != null) {
            operator.fill(queryExpBuilder);
        }

        return queryExpBuilder.toString();
    }

    @Override
    public T metric(String metric) {
        this.metric = metric;
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T labelEqual(String key, Object value) {
        addLabelFilter("=", value, key);
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T labelNotEqual(String key, Object value) {
        addLabelFilter("!=", value, key);
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T labelMatch(String key, Object value) {
        addLabelFilter("=~", value, key);
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T labelNotMatch(String key, Object value) {
        addLabelFilter("!~", value, key);
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T duration(Duration duration) {
        return durationSec(duration == null ? null : duration.getSeconds());
    }

    @Override
    public T durationSec(Long durationSec) {
        this.duration = durationSec == null ? null : durationSec + "s";
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T offset(Duration offset) {
        return offsetSec(offset == null ? null : offset.getSeconds());
    }

    @Override
    public T offsetSec(Long offsetSec) {
        if (offsetSec != null) {
            this.offset = offsetSec + "s";
        } else {
            this.offset = null;
        }
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T operate(BaudtimeOperator operator) {
        this.operator = operator;
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T updateOperate(Function<BaudtimeOperator, BaudtimeOperator> operatorUpdater) {
        operator = operatorUpdater.apply(operator);
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T queryTimeout(Duration queryTimeout) {
        queryTimeoutSec = queryTimeout.getSeconds();
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public T client(Client baudtimeClient) {
        this.baudtimeClient = baudtimeClient;
        //noinspection unchecked
        return (T) this;
    }

    private void addLabelFilter(String operator, Object value, String key) {
        Objects.requireNonNull(key, "labelValue filter key is required");
        if (filters == null) {
            filters = new LinkedList<>();
        }
        filters.add(key + operator + "'" + (value == null ? "" : value) + "'");
    }

    protected void copyFields(AbstractBaudtimeQueryBuilder queryBuilder) {
        queryBuilder.metric = this.metric;
        if (filters != null) {
            queryBuilder.filters = new LinkedList<>(this.filters);
        }
        queryBuilder.operator = this.operator;
        queryBuilder.duration = this.duration;
        queryBuilder.offset = this.offset;
        queryBuilder.queryTimeoutSec = this.queryTimeoutSec;
        queryBuilder.baudtimeClient = this.baudtimeClient;
    }

}
