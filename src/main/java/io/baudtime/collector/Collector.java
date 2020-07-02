/*
 * Source: https://github.com/prometheus/client_java/tree/master/simpleclient
 */

package io.baudtime.collector;

import io.baudtime.message.Series;

import java.util.Collection;

public abstract class Collector {

    public static final double NANOSECONDS_PER_SECOND = 1E9;
    public static final double MILLISECONDS_PER_SECOND = 1E3;

    public abstract Collection<Series> collect();

    /**
     * Convert a double to its string representation in Go.
     */
    public static String doubleToGoString(double d) {
        if (d == Double.POSITIVE_INFINITY) {
            return "+Inf";
        }
        if (d == Double.NEGATIVE_INFINITY) {
            return "-Inf";
        }
        if (Double.isNaN(d)) {
            return "NaN";
        }
        return Double.toString(d);
    }
}
