/*
 * Source: https://github.com/prometheus/client_java/tree/master/simpleclient
 */

package io.baudtime.collector;

import io.baudtime.message.Series;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Delta extends SimpleCollector<Delta.Child> {

    Delta(Builder b) {
        super(b);
    }

    public static class Builder extends SimpleCollector.Builder<Builder, Delta> {
        @Override
        public Delta create() {
            return new Delta(this);
        }
    }

    /**
     * Return a Builder to allow configuration of a new Delta. Ensures required fields are provided.
     *
     * @param name The name of the metric
     */
    public static Builder builder(String name) {
        return new Builder().name(name);
    }

    /**
     * Return a Builder to allow configuration of a new Delta.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Child newChild() {
        return new Child();
    }

    /**
     * The value of a single Delta.
     * <p>
     * <em>Warning:</em> References to a Child become invalid after using
     * {@link SimpleCollector#remove} or {@link SimpleCollector#clear},
     */
    public static class Child {
        private final DoubleAdder value = new DoubleAdder();
        private double preVal;
        private ReentrantLock l = new ReentrantLock();

        /**
         * Increment the counter by 1.
         */
        public void inc() {
            inc(1);
        }

        /**
         * Increment the counter by the given amount.
         *
         * @throws IllegalArgumentException If amt is negative.
         */
        public void inc(double amt) {
            if (amt < 0) {
                throw new IllegalArgumentException("Amount to increment must be non-negative.");
            }
            value.add(amt);
        }

        /**
         * Get the value of the delta.
         */
        public double get() {
            double curV, preV;

            l.lock();
            try {
                curV = value.sum();
                preV = preVal;
                preVal = curV;
            } finally {
                l.unlock();
            }

            return curV - preV;
        }
    }

    // Convenience methods.

    public void inc() {
        inc(1);
    }

    /**
     * @throws IllegalArgumentException If amt is negative.
     */
    public void inc(double amt) {
        noLabelsChild.inc(amt);
    }

    /**
     * Get the value of the delta.
     */
    public double get() {
        return noLabelsChild.get();
    }

    @Override
    public Collection<Series> collect() {
        List<Series> series = new ArrayList<Series>(children.size());

        Series.Builder builder = Series.newBuilder();
        for (Map.Entry<List<String>, Child> c : children.entrySet()) {
            List<String> labelValues = c.getKey();

            builder.setMetricName(metricName);
            for (int i = 0; i < labelNames.size(); i++) {
                builder.addLabel(labelNames.get(i), labelValues.get(i));
            }

            builder.addPoint(System.currentTimeMillis(), c.getValue().get());

            series.add(builder.build());

            builder.clear();
        }

        return series;
    }
}
