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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BaudtimeRangeQuery<R> implements BaudtimeQuery<R> {

    private Client baudtimeClient;

    private String queryExp;

    private long stepSec;

    private Date startTime;

    private Date endTime;

    private long queryTimeoutSec = 30;

    private BaudtimeQueryResponseConverter<QueryResponse, R> responseConverter;

    @Override
    public String getQueryExp() {
        return queryExp;
    }

    @Override
    public <T> T run(BaudtimeQueryResponseConverter<R, T> converter) throws BaudtimeQueryException {
        return converter.convert(this, run());
    }

    @Override
    public R run() throws BaudtimeQueryException {
        try {
            log.debug("run baudtime instant query. query={}", this);
            Objects.requireNonNull(baudtimeClient, "baudtimeClient is required.");
            QueryResponse response = baudtimeClient.rangeQuery(this.getQueryExp(), startTime, endTime, stepSec, queryTimeoutSec, TimeUnit.SECONDS);
            return responseConverter.convert(this, response);
        } catch (BaudtimeQueryException e) {
            throw e;
        } catch (Exception e) {
            throw new BaudtimeQueryException("query baudtime fault. query=" + toString(), e);
        }
    }

}
