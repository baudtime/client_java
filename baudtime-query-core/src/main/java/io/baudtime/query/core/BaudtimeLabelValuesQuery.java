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
import io.baudtime.message.LabelValuesResponse;
import io.baudtime.message.StatusCode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
@Data
@AllArgsConstructor
public class BaudtimeLabelValuesQuery {

    private Client baudtimeClient;

    private String targetLabel;

    private String constraint;

    private long queryTimeoutSec;

    public List<String> run() throws BaudtimeQueryException {
        try {
            log.debug("run baudtime label value query. query={}", this);
            Objects.requireNonNull(baudtimeClient, "baudtimeClient is required.");
            LabelValuesResponse response = baudtimeClient.labelValues(targetLabel, constraint, queryTimeoutSec, TimeUnit.SECONDS);

            if (response.getStatus() != StatusCode.Succeed) {
                throw new BaudtimeQueryException("query fault. status=" + response.getStatus() + " " + this.toString() + " errorMessage=" + response.getErrorMsg());
            }
            return response.getValues();
        } catch (BaudtimeQueryException e) {
            throw e;
        } catch (Exception e) {
            throw new BaudtimeQueryException("query baudtime fault. query=" + toString(), e);
        }

    }

    public <T> List<T> run(Function<String, T> itemConverter) throws BaudtimeQueryException {
        return run().stream()
                .map(itemConverter)
                .collect(Collectors.toList());
    }

}
