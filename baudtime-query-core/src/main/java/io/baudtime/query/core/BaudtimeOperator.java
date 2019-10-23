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

import lombok.RequiredArgsConstructor;


public interface BaudtimeOperator {

    // -------------------------- Aggregation operators -------------------------

    /**
     * calculate sum over dimensions
     */
    static BaudtimeOperator sum() {
        return SUM;
    }

    /**
     * select minimum over dimensions
     */
    static BaudtimeOperator min() {
        return MIN;
    }

    /**
     * select maximum over dimensions
     */
    static BaudtimeOperator max() {
        return MAX;
    }

    /**
     * calculate the average over dimensions
     */
    static BaudtimeOperator avg() {
        return AVG;
    }

    /**
     * calculate population standard deviation over dimensions
     */
    static BaudtimeOperator stddev() {
        return STDDEV;
    }

    /**
     * calculate population standard variance over dimensions
     */
    static BaudtimeOperator stdvar() {
        return STDVAR;
    }

    /**
     * count number of elements in the vector
     */
    static BaudtimeOperator count() {
        return COUNT;
    }

    /**
     * count number of elements with the same value
     */
    static BaudtimeOperator countValues(String value) {
        return new ArgsBaudtimeOperator("count_values") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.insert(0, "'" + value + "',");
            }
        };
    }

    /**
     * smallest k elements by sample value
     */
    static BaudtimeOperator bottomk(int limit) {
        return new ArgsBaudtimeOperator("bottomk") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.insert(0, limit + ",");
            }
        };
    }

    /**
     * largest k elements by sample value
     */
    static BaudtimeOperator topk(int limit) {
        return new ArgsBaudtimeOperator("topk") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.insert(0, limit + ",");
            }
        };
    }

    /**
     * calculate φ-quantile (0 ≤ φ ≤ 1) over dimensions
     */
    static BaudtimeOperator quantile(float phi) {

        return new ArgsBaudtimeOperator("quantile") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.insert(0, phi + ",");
            }
        };
    }

    BaudtimeOperator SUM = new SimpleBaudtimeOperator("sum");
    BaudtimeOperator MIN = new SimpleBaudtimeOperator("min");
    BaudtimeOperator MAX = new SimpleBaudtimeOperator("max");
    BaudtimeOperator AVG = new SimpleBaudtimeOperator("avg");
    BaudtimeOperator STDDEV = new SimpleBaudtimeOperator("stddev");
    BaudtimeOperator STDVAR = new SimpleBaudtimeOperator("stdvar");
    BaudtimeOperator COUNT = new SimpleBaudtimeOperator("count");


    // -------------------------- Arithmetic -------------------------

    static BaudtimeOperator add(Object expResource) {
        return new ArithmeticBaudtimeOperator("+", expResource);
    }

    static BaudtimeOperator sub(Object expResource) {
        return new ArithmeticBaudtimeOperator("-", expResource);
    }

    static BaudtimeOperator mcl(Object expResource) {
        return new ArithmeticBaudtimeOperator("*", expResource);
    }

    static BaudtimeOperator div(Object expResource) {
        return new ArithmeticBaudtimeOperator("/", expResource);
    }

    static BaudtimeOperator mod(Object expResource) {
        return new ArithmeticBaudtimeOperator("%", expResource);
    }

    static BaudtimeOperator pow(Object expResource) {
        return new ArithmeticBaudtimeOperator("^", expResource);
    }

    // -------------------------- vector matches -------------------------

    static BaudtimeOperator add(VectorMatchProvider provider, VectorMatchModifier modifier, Object expResource) {
        return new VectorMatchBaudtimeOperator("+", provider, modifier, expResource);
    }

    static BaudtimeOperator sub(VectorMatchProvider provider, VectorMatchModifier modifier, Object expResource) {
        return new VectorMatchBaudtimeOperator("-", provider, modifier, expResource);
    }

    static BaudtimeOperator mcl(VectorMatchProvider provider, VectorMatchModifier modifier, Object expResource) {
        return new VectorMatchBaudtimeOperator("*", provider, modifier, expResource);
    }

    static BaudtimeOperator div(VectorMatchProvider provider, VectorMatchModifier modifier, Object expResource) {
        return new VectorMatchBaudtimeOperator("/", provider, modifier, expResource);
    }

    static BaudtimeOperator mod(VectorMatchProvider provider, VectorMatchModifier modifier, Object expResource) {
        return new VectorMatchBaudtimeOperator("%", provider, modifier, expResource);
    }

    static BaudtimeOperator pow(VectorMatchProvider provider, VectorMatchModifier modifier, Object expResource) {
        return new VectorMatchBaudtimeOperator("^", provider, modifier, expResource);
    }


    // -------------------------- Comparison -------------------------

    static BaudtimeOperator equal(Object expResource) {
        return new ComparisonBaudtimeOperator("=", expResource);
    }

    static BaudtimeOperator notEqual(Object expResource) {
        return new ComparisonBaudtimeOperator("!=", expResource);
    }

    static BaudtimeOperator greaterThan(Object expResource) {
        return new ComparisonBaudtimeOperator(">", expResource);
    }

    static BaudtimeOperator lessThan(Object expResource) {
        return new ComparisonBaudtimeOperator("<", expResource);
    }

    static BaudtimeOperator greaterOrEqual(Object expResource) {
        return new ComparisonBaudtimeOperator(">=", expResource);
    }

    static BaudtimeOperator lessOrEqual(Object expResource) {
        return new ComparisonBaudtimeOperator("<=", expResource);
    }

    // -------------------------- Functions -------------------------

    static BaudtimeOperator abs() {
        return ABS;
    }

    static BaudtimeOperator absent() {
        return ABSENT;
    }

    static BaudtimeOperator ceil() {
        return CEIL;
    }

    static BaudtimeOperator changes() {
        return CHANGES;
    }

    static BaudtimeOperator clampMax(Number maxNumber) {
        return new ArgsBaudtimeOperator("clamp_max") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.append(",").append(maxNumber);
            }
        };
    }

    static BaudtimeOperator clampMin(Number minNumber) {
        return new ArgsBaudtimeOperator("clamp_min") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.append(",").append(minNumber);
            }
        };
    }

    static BaudtimeOperator dayOfMonth() {
        return DAY_OF_MONTH;
    }

    static BaudtimeOperator dayOfWeek() {
        return DAY_OF_WEEK;
    }

    static BaudtimeOperator daysInMonth() {
        return DAYS_IN_MONTH;
    }

    static BaudtimeOperator delta() {
        return DELTA;
    }

    static BaudtimeOperator deriv() {
        return DERIV;
    }

    static BaudtimeOperator exp() {
        return EXP;
    }

    static BaudtimeOperator floor() {
        return FLOOR;
    }

    static BaudtimeOperator histogramQuantile(float phi) {
        return new ArgsBaudtimeOperator("histogram_quantile") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.insert(0, phi + ",");
            }
        };
    }

    /**
     * holt_winters(v range-vector, sf scalar, tf scalar) produces a smoothed value for time series based on the
     * range in v. The lower the smoothing factor sf, the more importance is given to old data. The higher the
     * trend factor tf, the more trends in the data is considered. Both sf and tf must be between 0 and 1.
     * <p>
     * holt_winters should only be used with gauges.
     */
    static BaudtimeOperator holtWinters(float sf, float tf) {
        return new ArgsBaudtimeOperator("holt_winters") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.append(",").append(sf).append(",").append(tf);
            }
        };
    }

    static BaudtimeOperator hour() {
        return HOUR;
    }

    static BaudtimeOperator idelta() {
        return IDELTA;
    }

    static BaudtimeOperator increase() {
        return INCREASE;
    }

    static BaudtimeOperator irate() {
        return IRATE;
    }

    /**
     * For each timeseries in v, label_join(v instant-vector, dst_label string, separator string, src_label_1 string,
     * src_label_2 string, ...) joins all the values of all the src_labels using separator and returns the timeseries
     * with the labelValue dst_label containing the joined value. There can be any number of src_labels in this function.
     * <p>
     * This example will return a vector with each time series having a foo labelValue with the value a,b,c added to it:
     */
    static BaudtimeOperator labelJoin(String dstLabel, String separator, String... srcLabels) {
        return new ArgsBaudtimeOperator("label_join") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.append(",").append(dstLabel).append(",").append(separator);
                for (String srcLabel : srcLabels) {
                    queryExp.append(",").append(srcLabel);
                }
            }
        };
    }

    /**
     * For each timeseries in v, label_replace(v instant-vector, dst_label string, replacement string, src_label
     * string, regex string) matches the regular expression regex against the labelValue src_label. If it matches, then
     * the timeseries is returned with the labelValue dst_label replaced by the expansion of replacement. $1 is replaced
     * with the first matching subgroup, $2 with the second etc. If the regular expression doesn't match then the
     * timeseries is returned unchanged.
     * <p>
     * This example will return a vector with each time series having a foo labelValue with the value a added to it:
     */
    static BaudtimeOperator labelReplace(String dstLabel, String replacement, String srcLabel, String regex) {
        return new ArgsBaudtimeOperator("label_replace") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.append(",").append(dstLabel)
                        .append(",").append(replacement)
                        .append(",").append(srcLabel)
                        .append(",").append(regex);
            }
        };
    }

    static BaudtimeOperator ln() {
        return LN;
    }

    static BaudtimeOperator log2() {
        return LOG2;
    }

    static BaudtimeOperator log10() {
        return LOG10;
    }

    static BaudtimeOperator minute() {
        return MINUTE;
    }

    static BaudtimeOperator month() {
        return MONTH;
    }

    /**
     * predict_linear(v range-vector, t scalar) predicts the value of time series t seconds from now, based on the
     * range vector v, using simple linear regression.
     * <p>
     * predict_linear should only be used with gauges.
     */
    static BaudtimeOperator predictLinear(int seconds) {
        return new ArgsBaudtimeOperator("predict_linear") {
            @Override
            void processArgs(StringBuilder queryExp) {
                queryExp.append(",").append(seconds);
            }
        };
    }

    static BaudtimeOperator rate() {
        return RATE;
    }

    static BaudtimeOperator resets() {
        return RESETS;
    }

    static BaudtimeOperator round() {
        return ROUND;
    }

    static BaudtimeOperator scalar() {
        return SCALAR;
    }

    static BaudtimeOperator sort() {
        return SORT;
    }

    static BaudtimeOperator sortDesc() {
        return SORT_DESC;
    }

    static BaudtimeOperator sqrt() {
        return SQRT;
    }

    static BaudtimeOperator time() {
        return TIME;
    }

    static BaudtimeOperator timestamp() {
        return TIMESTAMP;
    }

    static BaudtimeOperator vector() {
        return VECTOR;
    }

    static BaudtimeOperator year() {
        return YEAR;
    }

    BaudtimeOperator ABS = new SimpleBaudtimeOperator("abs");
    BaudtimeOperator ABSENT = new SimpleBaudtimeOperator("absent");
    BaudtimeOperator CEIL = new SimpleBaudtimeOperator("ceil");
    BaudtimeOperator CHANGES = new SimpleBaudtimeOperator("changes");
    BaudtimeOperator DAY_OF_MONTH = new SimpleBaudtimeOperator("day_of_month");
    BaudtimeOperator DAY_OF_WEEK = new SimpleBaudtimeOperator("day_of_week");
    BaudtimeOperator DAYS_IN_MONTH = new SimpleBaudtimeOperator("days_in_month");
    BaudtimeOperator DELTA = new SimpleBaudtimeOperator("delta");
    BaudtimeOperator DERIV = new SimpleBaudtimeOperator("deriv");
    BaudtimeOperator EXP = new SimpleBaudtimeOperator("exp");
    BaudtimeOperator FLOOR = new SimpleBaudtimeOperator("floor");
    BaudtimeOperator HOUR = new SimpleBaudtimeOperator("hour");
    BaudtimeOperator IDELTA = new SimpleBaudtimeOperator("idelta");
    BaudtimeOperator INCREASE = new SimpleBaudtimeOperator("increase");
    BaudtimeOperator IRATE = new SimpleBaudtimeOperator("irate");
    BaudtimeOperator LN = new SimpleBaudtimeOperator("ln");
    BaudtimeOperator LOG2 = new SimpleBaudtimeOperator("log2");
    BaudtimeOperator LOG10 = new SimpleBaudtimeOperator("log10");
    BaudtimeOperator MINUTE = new SimpleBaudtimeOperator("minute");
    BaudtimeOperator MONTH = new SimpleBaudtimeOperator("month");
    BaudtimeOperator PREDICT_LINEAR = new SimpleBaudtimeOperator("predict_linear");
    BaudtimeOperator RATE = new SimpleBaudtimeOperator("rate");
    BaudtimeOperator RESETS = new SimpleBaudtimeOperator("resets");
    BaudtimeOperator ROUND = new SimpleBaudtimeOperator("round");
    BaudtimeOperator SCALAR = new SimpleBaudtimeOperator("scalar");
    BaudtimeOperator SORT = new SimpleBaudtimeOperator("sort");
    BaudtimeOperator SORT_DESC = new SimpleBaudtimeOperator("sort_desc");
    BaudtimeOperator SQRT = new SimpleBaudtimeOperator("sqrt");
    BaudtimeOperator TIME = new SimpleBaudtimeOperator("time");
    BaudtimeOperator TIMESTAMP = new SimpleBaudtimeOperator("timestamp");
    BaudtimeOperator VECTOR = new SimpleBaudtimeOperator("vector");
    BaudtimeOperator YEAR = new SimpleBaudtimeOperator("year");

    // -------------------------- Aggregation Over Time -------------------------

    static BaudtimeOperator avgOverTime() {
        return AVG_OVER_TIME;
    }

    static BaudtimeOperator minOverTime() {
        return MIN_OVER_TIME;
    }

    static BaudtimeOperator maxOverTime() {
        return MAX_OVER_TIME;
    }

    static BaudtimeOperator sumOverTime() {
        return SUM_OVER_TIME;
    }

    static BaudtimeOperator countOverTime() {
        return COUNT_OVER_TIME;
    }

    static BaudtimeOperator quantileOverTime() {
        return QUANTILE_OVER_TIME;
    }

    static BaudtimeOperator stddevOverTime() {
        return STDDEV_OVER_TIME;
    }

    static BaudtimeOperator stdvarOverTime() {
        return STDVAR_OVER_TIME;
    }

    BaudtimeOperator AVG_OVER_TIME = new SimpleBaudtimeOperator("avg_over_time");
    BaudtimeOperator MIN_OVER_TIME = new SimpleBaudtimeOperator("min_over_time");
    BaudtimeOperator MAX_OVER_TIME = new SimpleBaudtimeOperator("max_over_time");
    BaudtimeOperator SUM_OVER_TIME = new SimpleBaudtimeOperator("sum_over_time");
    BaudtimeOperator COUNT_OVER_TIME = new SimpleBaudtimeOperator("count_over_time");
    BaudtimeOperator QUANTILE_OVER_TIME = new SimpleBaudtimeOperator("quantile_over_time");
    BaudtimeOperator STDDEV_OVER_TIME = new SimpleBaudtimeOperator("stddev_over_time");
    BaudtimeOperator STDVAR_OVER_TIME = new SimpleBaudtimeOperator("stdvar_over_time");

    void fill(StringBuilder queryExp);

    default BaudtimeOperator compose(BaudtimeOperator before) {
        return queryExp -> {
            before.fill(queryExp);
            fill(queryExp);
        };
    }

    default BaudtimeOperator andThen(BaudtimeOperator after) {
        return queryExp -> {
            fill(queryExp);
            after.fill(queryExp);
        };
    }

    default BaudtimeOperator groupBy(String... labels) {
        return queryExp -> {
            fill(queryExp);
            queryExp.append(" by (").append(String.join(",", labels)).append(")");
        };
    }

    default BaudtimeOperator without(String... labels) {
        return queryExp -> {
            fill(queryExp);
            queryExp.append(" without (").append(String.join(",", labels)).append(")");
        };
    }

    @RequiredArgsConstructor
    class SimpleBaudtimeOperator implements BaudtimeOperator {

        private final String name;

        @Override
        public void fill(StringBuilder queryExp) {
            queryExp.insert(0, name + "(").append(")");
        }

    }

    @RequiredArgsConstructor
    abstract class ArgsBaudtimeOperator implements BaudtimeOperator {

        private final String name;

        @Override
        public void fill(StringBuilder queryExp) {
            processArgs(queryExp);
            queryExp.insert(0, name + "(").append(")");
        }

        abstract void processArgs(StringBuilder queryExp);

    }

    @RequiredArgsConstructor
    class ArithmeticBaudtimeOperator implements BaudtimeOperator {

        private final String operator;

        private final Object expResource;

        @Override
        public void fill(StringBuilder queryExp) {
            queryExp.insert(0, "(").append(" ").append(operator).append(" ");
            if (expResource instanceof BaudtimeQueryBuilder) {
                queryExp.append(((BaudtimeQueryBuilder) expResource).queryExp());
            } else if (expResource instanceof BaudtimeQuery) {
                queryExp.append(((BaudtimeQuery) expResource).getQueryExp());
            } else {
                queryExp.append(expResource.toString());
            }
            queryExp.append(")");
        }

    }

    @RequiredArgsConstructor
    class ComparisonBaudtimeOperator implements BaudtimeOperator {

        private final String operator;

        private final Object expResource;

        @Override
        public void fill(StringBuilder queryExp) {
            queryExp.append(" ").append(operator).append(" ");
            if (expResource instanceof BaudtimeQueryBuilder) {
                queryExp.append(((BaudtimeQueryBuilder) expResource).queryExp());
            } else if (expResource instanceof BaudtimeQuery) {
                queryExp.append(((BaudtimeQuery) expResource).getQueryExp());
            } else {
                queryExp.append(expResource.toString());
            }
        }

    }

    class VectorMatchBaudtimeOperator extends ComparisonBaudtimeOperator {

        public VectorMatchBaudtimeOperator(String operator, VectorMatchProvider provider, VectorMatchModifier modifier, Object expResource) {
            super(operator + " " + provider + " " + modifier, expResource);
        }

    }

    interface VectorMatchProvider {

        static VectorMatchProvider on(String... labels) {
            return new VectorMatchProvider() {
                @Override
                public String toString() {
                    return "on(" + String.join(",", labels) + ")";
                }
            };
        }

        static VectorMatchProvider ignoring(String... labels) {
            return new VectorMatchProvider() {
                @Override
                public String toString() {
                    return "ignoring(" + String.join(",", labels) + ")";
                }
            };
        }

    }

    interface VectorMatchModifier {

        static VectorMatchModifier groupLeft(String... labels) {
            return new VectorMatchModifier() {
                @Override
                public String toString() {
                    return "group_left(" + String.join(",", labels) + ")";
                }
            };
        }

        static VectorMatchModifier groupRight(String... labels) {
            return new VectorMatchModifier() {
                @Override
                public String toString() {
                    return "group_right(" + String.join(",", labels) + ")";
                }
            };
        }
    }

}
