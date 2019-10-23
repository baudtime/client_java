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

import java.time.Duration;
import java.util.function.Function;


public interface BaudtimeQueryBuilder<T extends BaudtimeQueryBuilder<T, Q, R>, Q extends BaudtimeQuery<R>, R> extends LabelFilterBuilder<T> {

    String queryExp();

    Q build();

    T metric(String metric);

    T duration(Duration duration);

    T durationSec(Long durationSec);

    T offset(Duration offset);

    T offsetSec(Long offsetSec);

    T operate(BaudtimeOperator operator);

    T updateOperate(Function<BaudtimeOperator, BaudtimeOperator> operatorUpdater);

    T queryTimeout(Duration queryTimeout);

    T client(Client baudtimeClient);

    T copy();

}