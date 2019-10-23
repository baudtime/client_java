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

import java.math.BigDecimal;
import java.util.function.Function;


public class DataPoints {

    public static final String NAN = "NaN";
    public static final String INF = "+Inf";

    public static final int TIME_INDEX = 0;
    public static final int VALUE_INDEX = 1;

    public static long getUnixTimestamp(JSONArray dataPoint) {
        return Math.round(dataPoint.getDoubleValue(TIME_INDEX) * 1000);
    }

    public static int getIntValue(JSONArray dataPoint) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return 0;
        } else if (valueString.equals(INF)) {
            return 0;
        }
        return dataPoint.getIntValue(VALUE_INDEX);
    }

    public static long getLongValue(JSONArray dataPoint) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return 0;
        } else if (valueString.equals(INF)) {
            return 0;
        }
        return dataPoint.getLongValue(VALUE_INDEX);
    }

    public static float getFloatValue(JSONArray dataPoint) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return Float.NaN;
        } else if (valueString.equals(INF)) {
            return Float.POSITIVE_INFINITY;
        }
        return dataPoint.getFloatValue(VALUE_INDEX);
    }

    public static double getDoubleValue(JSONArray dataPoint) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return Double.NaN;
        } else if (valueString.equals(INF)) {
            return Double.POSITIVE_INFINITY;
        }
        return dataPoint.getDoubleValue(VALUE_INDEX);
    }

    public static Integer getInteger(JSONArray dataPoint) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return null;
        } else if (valueString.equals(INF)) {
            return null;
        }
        return dataPoint.getIntValue(VALUE_INDEX);
    }

    public static Long getLong(JSONArray dataPoint) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return null;
        } else if (valueString.equals(INF)) {
            return null;
        }
        return dataPoint.getLongValue(VALUE_INDEX);
    }

    public static BigDecimal getBigDecimalValue(JSONArray dataPoint) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return null;
        } else if (valueString.equals(INF)) {
            return null;
        }
        return dataPoint.getBigDecimal(VALUE_INDEX);
    }

    public static <T> T asBigDecimal(JSONArray dataPoint, Function<BigDecimal, T> filter) {
        String valueString = dataPoint.getString(VALUE_INDEX);
        if (valueString.equals(NAN)) {
            return null;
        } else if (valueString.equals(INF)) {
            return null;
        }
        return filter.apply(dataPoint.getBigDecimal(VALUE_INDEX));
    }

}
