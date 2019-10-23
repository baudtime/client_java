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

import io.baudtime.message.QueryResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


public class BaudtimeRangeQueryBuilder<R> extends AbstractBaudtimeQueryBuilder<BaudtimeRangeQueryBuilder<R>, BaudtimeRangeQuery<R>, R> {

    private Long stepSec;

    private Date startTime;

    private Date endTime;

    public BaudtimeRangeQueryBuilder(BaudtimeQueryResponseConverter<QueryResponse, R> responseConverter) {
        super(responseConverter);
    }

    public BaudtimeRangeQueryBuilder<R> timeOf(Duration step, Duration duration) {
        return timeOf(step, duration, System.currentTimeMillis());
    }

    public BaudtimeRangeQueryBuilder<R> timeOf(Duration step, Instant startTime, Instant endTime) {
        return timeOf(step, startTime.toEpochMilli(), endTime.toEpochMilli(), TimeUnit.MILLISECONDS);
    }

    public BaudtimeRangeQueryBuilder<R> timeOf(Duration step, long startTime, long endTime, TimeUnit timeUnit) {
        Objects.requireNonNull(step);
        Objects.requireNonNull(timeUnit);
        this.stepSec = step.getSeconds();
        long stepMillis = step.toMillis();
        this.startTime = formatQueryDate(stepMillis, timeUnit.toMillis(startTime));
        this.endTime = formatQueryDate(stepMillis, timeUnit.toMillis(endTime));
        return this;
    }

    public BaudtimeRangeQueryBuilder<R> timeOf(Duration step, Duration duration, long timestamp) {
        Objects.requireNonNull(step);
        Objects.requireNonNull(duration);
        this.stepSec = step.getSeconds();
        long stepMillis = step.toMillis();
        this.startTime = formatQueryDate(stepMillis, timestamp - duration.toMillis());
        this.endTime = formatQueryDate(stepMillis, timestamp);
        return this;
    }

    private Date formatQueryDate(long stepMillis, long dateTimestamp) {
        return new Date(dateTimestamp / stepMillis * stepMillis);
    }

    @Override
    public BaudtimeRangeQuery<R> build() {
        Objects.requireNonNull(stepSec, "step is required.");
        Objects.requireNonNull(startTime, "startTime is required.");
        Objects.requireNonNull(endTime, "endTime is required.");
        return new BaudtimeRangeQuery<>(baudtimeClient, queryExp(), stepSec, startTime, endTime, queryTimeoutSec, responseConverter);
    }

    @Override
    public BaudtimeRangeQueryBuilder<R> copy() {
        return copy(responseConverter);
    }

    public <R2> BaudtimeRangeQueryBuilder<R2> copy(BaudtimeQueryResponseConverter<QueryResponse, R2> converter) {
        BaudtimeRangeQueryBuilder<R2> copy = new BaudtimeRangeQueryBuilder<>(converter);
        copy.stepSec = this.stepSec;
        copy.startTime = this.startTime;
        copy.endTime = this.endTime;
        copyFields(copy);
        return copy;
    }

    public <R2> BaudtimeRangeQueryBuilder<R2> responseMap(BaudtimeQueryResponseConverter<R, R2> converter) {
        return copy(responseConverter.andThen(converter));
    }

}
