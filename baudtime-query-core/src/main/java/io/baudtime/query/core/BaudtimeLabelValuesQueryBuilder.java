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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


public class BaudtimeLabelValuesQueryBuilder implements LabelFilterBuilder<BaudtimeLabelValuesQueryBuilder> {

    private String targetLabel;

    private List<String> filters;

    private long queryTimeoutSec = 30;

    protected Client baudtimeClient;

    public BaudtimeLabelValuesQueryBuilder targetLabel(String targetLabel) {
        this.targetLabel = targetLabel;
        return this;
    }

    @Override
    public BaudtimeLabelValuesQueryBuilder labelEqual(String key, Object value) {
        addLabelFilter("=", value, key);
        return this;
    }

    @Override
    public BaudtimeLabelValuesQueryBuilder labelNotEqual(String key, Object value) {
        addLabelFilter("!=", value, key);
        return this;
    }

    @Override
    public BaudtimeLabelValuesQueryBuilder labelMatch(String key, Object value) {
        addLabelFilter("=~", value, key);
        return this;
    }

    @Override
    public BaudtimeLabelValuesQueryBuilder labelNotMatch(String key, Object value) {
        addLabelFilter("!~", value, key);
        return this;
    }

    public BaudtimeLabelValuesQueryBuilder queryTimeout(Duration queryTimeout) {
        queryTimeoutSec = queryTimeout.getSeconds();
        return this;
    }

    public BaudtimeLabelValuesQueryBuilder client(Client baudtimeClient) {
        this.baudtimeClient = baudtimeClient;
        return this;
    }

    private void addLabelFilter(String operator, Object value, String key) {
        Objects.requireNonNull(key, "labelValue filter key is required");
        if (filters == null) {
            filters = new LinkedList<>();
        }
        filters.add(key + operator + "'" + (value == null ? "" : value) + "'");
    }

    public BaudtimeLabelValuesQuery build() {

        Objects.requireNonNull(targetLabel, "targetLabel is required.");

        String constraint;
        if (filters != null && !filters.isEmpty()) {
            constraint = "{" + String.join(",", filters) + "}";
        } else {
            constraint = null;
        }

        return new BaudtimeLabelValuesQuery(baudtimeClient, targetLabel, constraint, queryTimeoutSec);
    }

}
