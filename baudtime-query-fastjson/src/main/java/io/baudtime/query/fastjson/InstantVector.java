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

package io.baudtime.query.fastjson;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.math.BigDecimal;
import java.util.function.Function;


@Data
public class InstantVector {

    private JSONObject metric;

    private JSONArray value;

    public long getUnixTimestamp() {
        return DataPoints.getUnixTimestamp(value);
    }

    public int getIntValue() {
        return DataPoints.getIntValue(value);
    }

    public long getLongValue() {
        return DataPoints.getLongValue(value);
    }

    public float getFloatValue() {
        return DataPoints.getFloatValue(value);
    }

    public double getDoubleValue() {
        return DataPoints.getDoubleValue(value);
    }

    public Integer getInteger() {
        return DataPoints.getInteger(value);
    }

    public Long getLong() {
        return DataPoints.getLong(value);
    }

    public BigDecimal getBigDecimalValue() {
        return DataPoints.getBigDecimalValue(value);
    }

    public <T> T asBigDecimal(Function<BigDecimal, T> filter) {
        return DataPoints.asBigDecimal(value, filter);
    }

}
