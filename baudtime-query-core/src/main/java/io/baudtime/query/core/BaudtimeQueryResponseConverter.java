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

import java.util.Objects;


public interface BaudtimeQueryResponseConverter<O, T> {

    BaudtimeQueryResponseConverter<QueryResponse, QueryResponse> NOOP = (query, response) -> response;

    T convert(BaudtimeQuery query, O value) throws BaudtimeQueryException;

    default <O2> BaudtimeQueryResponseConverter<O2, T> compose(BaudtimeQueryResponseConverter<? super O2, ? extends O> before) {
        Objects.requireNonNull(before);
        return (query, value) -> convert(query, before.convert(query, value));
    }

    default <T2> BaudtimeQueryResponseConverter<O, T2> andThen(BaudtimeQueryResponseConverter<? super T, ? extends T2> after) {
        Objects.requireNonNull(after);
        return (query, value) -> after.convert(query, convert(query, value));
    }

}

