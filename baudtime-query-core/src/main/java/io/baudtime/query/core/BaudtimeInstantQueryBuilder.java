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


public class BaudtimeInstantQueryBuilder<R> extends AbstractBaudtimeQueryBuilder<BaudtimeInstantQueryBuilder<R>, BaudtimeInstantQuery<R>, R> {

    private Date time;

    public BaudtimeInstantQueryBuilder(BaudtimeQueryResponseConverter<QueryResponse, R> responseConverter) {
        super(responseConverter);
    }

    public BaudtimeInstantQueryBuilder<R> timeOf(Instant startTime, Instant endTime) {
        return timeOf(startTime.toEpochMilli(), endTime.toEpochMilli(), TimeUnit.MILLISECONDS);
    }

    public BaudtimeInstantQueryBuilder<R> timeOf(long startTime, long endTime, TimeUnit timeUnit) {
        return timeOf(timeUnit.toSeconds(endTime - startTime), timeUnit.toMillis(endTime));
    }

    public BaudtimeInstantQueryBuilder<R> timeOf(Duration duration) {
        return timeOf(duration, null);
    }

    public BaudtimeInstantQueryBuilder<R> timeOf(Duration duration, Long timestamp) {
        return timeOf(duration == null ? null : duration.getSeconds(), timestamp);
    }

    public BaudtimeInstantQueryBuilder<R> timeOf(Long durationSec, Long timestamp) {
        durationSec(durationSec);
        this.time = new Date(timestamp == null ? System.currentTimeMillis() : timestamp);
        return this;
    }

    public BaudtimeInstantQueryBuilder<R> timeOf(long timestamp) {
        this.time = new Date(timestamp);
        return this;
    }

    @Override
    public BaudtimeInstantQuery<R> build() {
        Objects.requireNonNull(time, "time is required.");
        return new BaudtimeInstantQuery<>(baudtimeClient, queryExp(), time, queryTimeoutSec, responseConverter);
    }

    @Override
    public BaudtimeInstantQueryBuilder<R> copy() {
        return copy(responseConverter);
    }

    public <R2> BaudtimeInstantQueryBuilder<R2> copy(BaudtimeQueryResponseConverter<QueryResponse, R2> converter) {
        BaudtimeInstantQueryBuilder<R2> copy = new BaudtimeInstantQueryBuilder<>(converter);
        copy.time = this.time;
        copyFields(copy);
        return copy;
    }

    /**
     * please use {@link #responseMap(BaudtimeQueryResponseConverter)}
     */
    @Deprecated
    public <R2> BaudtimeInstantQueryBuilder<R2> responseConverter(BaudtimeQueryResponseConverter<R, R2> converter) {
        return copy(responseConverter.andThen(converter));
    }

    public <R2> BaudtimeInstantQueryBuilder<R2> responseMap(BaudtimeQueryResponseConverter<R, R2> converter) {
        return copy(responseConverter.andThen(converter));
    }


}
