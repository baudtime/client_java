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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.baudtime.message.QueryResponse;
import io.baudtime.message.StatusCode;
import io.baudtime.query.core.BaudtimeQueryException;
import io.baudtime.query.core.BaudtimeQueryResponseConverter;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;


public class FastjsonResponseConverters {

    public static final BaudtimeQueryResponseConverter<QueryResponse, JSONObject> CHECK_STATE_AND_PARSE_JSON = (query, response) -> {

        if (response.getStatus() != StatusCode.Succeed) {
            throw new BaudtimeQueryException("query fault. status=" + response.getStatus() + " " + query.toString() + " errorMessage=" + response.getErrorMsg());
        }

        return JSON.parseObject(response.getResult());

    };

    /**
     * Response result Example:
     * <pre>
     * {
     *     "resultType": "matrix",
     *     "result": [
     *         {
     *             "metric": {
     *                 "APP": "24",
     *                 "INST": "58",
     *                 "RES": "raw",
     *                 "_AG": "pts",
     *                 "__name__": "pf197"
     *             },
     *             "values": [
     *                 [
     *                     1559029052.421,
     *                     "0"
     *                 ]
     *             ]
     *         }
     *     ]
     * }
     * </pre>
     */
    public static final BaudtimeQueryResponseConverter<QueryResponse, JSONArray> MATRIX_JSON = ((BaudtimeQueryResponseConverter<JSONObject, JSONArray>) (query, result) -> {

        String resultType = result.getString("resultType");
        if (!Objects.equals(resultType, "matrix")) {
            throw new BaudtimeQueryException("unsupported resultType '" + resultType + "'. " + query.toString());
        }

        return result.getJSONArray("result");

    }).compose(CHECK_STATE_AND_PARSE_JSON);

    public static final BaudtimeQueryResponseConverter<QueryResponse, List<RangeVector>> MATRIX = MATRIX_JSON.andThen((query, result) -> result.toJavaList(RangeVector.class));


    /**
     * Response result Example:
     * <pre>
     * {
     *    "status" : "success",
     *    "data" : {
     *       "resultType" : "vector",
     *       "result" : [
     *          {
     *             "metric" : {
     *                "__name__" : "up",
     *                "job" : "prometheus",
     *                "instance" : "localhost:9090"
     *             },
     *             "value": [ 1435781451.781, "1" ]
     *          },
     *          {
     *             "metric" : {
     *                "__name__" : "up",
     *                "job" : "node",
     *                "instance" : "localhost:9100"
     *             },
     *             "value" : [ 1435781451.781, "0" ]
     *          }
     *       ]
     *    }
     * }
     * </pre>
     */
    public static final BaudtimeQueryResponseConverter<QueryResponse, JSONArray> VECTOR_JSON = ((BaudtimeQueryResponseConverter<JSONObject, JSONArray>) (query, result) -> {

        String resultType = result.getString("resultType");
        if (!Objects.equals(resultType, "vector")) {
            throw new BaudtimeQueryException("unsupported resultType '" + resultType + "'. " + query.toString());
        }

        return result.getJSONArray("result");

    }).compose(CHECK_STATE_AND_PARSE_JSON);

    public static final BaudtimeQueryResponseConverter<QueryResponse, List<InstantVector>> VECTOR = VECTOR_JSON.andThen((query, result) -> result.toJavaList(InstantVector.class));

    public static final BaudtimeQueryResponseConverter<QueryResponse, InstantVector> SINGLE_VECTOR = VECTOR.andThen((query, result) -> result.get(0));

    public static <T> BaudtimeQueryResponseConverter<QueryResponse, T> singleVectorValue(Function<InstantVector, T> valueExtractor) {
        return SINGLE_VECTOR.andThen((query, value) -> valueExtractor.apply(value));
    }

}
