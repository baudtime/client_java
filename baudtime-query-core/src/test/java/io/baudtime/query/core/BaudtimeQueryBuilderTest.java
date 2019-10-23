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
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

public class BaudtimeQueryBuilderTest {

    @Test
    public void instantQueryBuilderTest() {
        BaudtimeInstantQuery query = new BaudtimeInstantQueryBuilder<>(BaudtimeQueryResponseConverter.NOOP)
                .timeOf(Duration.ofSeconds(30))
                .offset(Duration.ofDays(1))
                .metric("pf")

                // tags
                .labelEqual("INST", 51)
                .labelMatch("SUB", "invokes|tp99")
                .labelNotMatch("_AG", ".*")
                .labelNotEqual("RES", "raw")
                .labelNotEqual("RES", null)

                // function
                .operate(BaudtimeOperator.topk(10).compose(BaudtimeOperator.sumOverTime().groupBy("APP")).without("RES"))

                .build();

        String expectedQueryExp = "topk(10,sum_over_time(pf{INST='51',SUB=~'invokes|tp99',_AG!~'.*',RES!='raw',RES!=''}[30s] offset 86400s) by (APP)) without (RES)";

        assertEquals(expectedQueryExp, query.getQueryExp());
    }

    @Test
    public void rangeQueryBuilderTest() {
        BaudtimeRangeQuery query = new BaudtimeRangeQueryBuilder<>(BaudtimeQueryResponseConverter.NOOP)
                .timeOf(Duration.ofSeconds(10), Duration.ofMinutes(30))

                // tags
                .labelEqual("MID", 123)
                .labelEqual("INST", 51)
                .labelMatch("SUB", "invokes|tp99")
                .labelNotMatch("_AG", ".*")
                .labelNotEqual("RES", "raw")
                .labelNotEqual("RES", null)

                // function
                .operate(BaudtimeOperator.sumOverTime().groupBy("APP").andThen(BaudtimeOperator.sum().without("RES")))

                .build();

        String expectedQueryExp = "sum(sum_over_time({MID='123',INST='51',SUB=~'invokes|tp99',_AG!~'.*',RES!='raw',RES!=''}) by (APP)) without (RES)";

        assertEquals(expectedQueryExp, query.getQueryExp());
    }

    @Test
    public void instantQueryBuilderTest2() {

        BaudtimeInstantQueryBuilder<QueryResponse> baseQueryBuilder = new BaudtimeInstantQueryBuilder<>(BaudtimeQueryResponseConverter.NOOP)
                .timeOf(Duration.ofSeconds(30))
                .metric("pf")
                .operate(BaudtimeOperator.sumOverTime().andThen(BaudtimeOperator.sum()).groupBy("__name__"));

        BaudtimeInstantQueryBuilder<QueryResponse> invokesQuery = baseQueryBuilder.copy()
                .labelEqual("SUB", "invokes");

        BaudtimeInstantQuery query = baseQueryBuilder.copy()
                .labelEqual("SUB", "elapsed")
                .updateOperate(fun -> fun.andThen(BaudtimeOperator.div(invokesQuery).andThen(BaudtimeOperator.topk(10))))
                .build();

        String expectedQueryExp = "topk(10,(sum(sum_over_time(pf{SUB='elapsed'}[30s])) by (__name__) / sum(sum_over_time(pf{SUB='invokes'}[30s])) by (__name__)))";

        assertEquals(expectedQueryExp, query.getQueryExp());
    }

}