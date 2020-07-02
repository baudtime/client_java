/*
 * Source: https://github.com/prometheus/client_java/tree/master/simpleclient
 */

package io.baudtime.collector;

import io.baudtime.collector.CKMSQuantiles.Quantile;
import io.baudtime.message.Label;
import io.baudtime.message.Point;
import io.baudtime.message.Series;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class Summary extends SimpleCollector<Summary.Child> {

    final List<Quantile> quantiles; // Can be empty, but can never be null.
    final long maxAgeSeconds;
    final int ageBuckets;

    Summary(Builder b) {
        super(b);
        quantiles = Collections.unmodifiableList(new ArrayList<Quantile>(b.quantiles));
        this.maxAgeSeconds = b.maxAgeSeconds;
        this.ageBuckets = b.ageBuckets;
        initializeNoLabelsChild();
    }

    public static class Builder extends SimpleCollector.Builder<Builder, Summary> {

        private final List<Quantile> quantiles = new ArrayList<Quantile>();
        private long maxAgeSeconds = TimeUnit.MINUTES.toSeconds(10);
        private int ageBuckets = 5;

        public Builder quantile(double quantile, double error) {
            if (quantile < 0.0 || quantile > 1.0) {
                throw new IllegalArgumentException("Quantile " + quantile + " invalid: Expected number between 0.0 and 1.0.");
            }
            if (error < 0.0 || error > 1.0) {
                throw new IllegalArgumentException("Error " + error + " invalid: Expected number between 0.0 and 1.0.");
            }
            quantiles.add(new Quantile(quantile, error));
            return this;
        }

        public Builder maxAgeSeconds(long maxAgeSeconds) {
            if (maxAgeSeconds <= 0) {
                throw new IllegalArgumentException("maxAgeSeconds cannot be " + maxAgeSeconds);
            }
            this.maxAgeSeconds = maxAgeSeconds;
            return this;
        }

        public Builder ageBuckets(int ageBuckets) {
            if (ageBuckets <= 0) {
                throw new IllegalArgumentException("ageBuckets cannot be " + ageBuckets);
            }
            this.ageBuckets = ageBuckets;
            return this;
        }

        @Override
        public Summary create() {
            for (String label : labelNames) {
                if (label.equals("quantile")) {
                    throw new IllegalStateException("Summary cannot have a label named 'quantile'.");
                }
            }
            dontInitializeNoLabelsChild = true;
            return new Summary(this);
        }
    }

    /**
     * Return a Builder to allow configuration of a new Summary. Ensures required fields are provided.
     *
     * @param name The name of the metric
     */
    public static Builder builder(String name) {
        return new Builder().name(name);
    }

    /**
     * Return a Builder to allow configuration of a new Summary.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected Child newChild() {
        return new Child(quantiles, maxAgeSeconds, ageBuckets);
    }


    /**
     * Represents an event being timed.
     */
    public static class Timer implements Closeable {
        private final Child child;
        private final long start;

        private Timer(Child child, long start) {
            this.child = child;
            this.start = start;
        }

        /**
         * Observe the amount of time in seconds since {@link Child#startTimer} was called.
         *
         * @return Measured duration in seconds since {@link Child#startTimer} was called.
         */
        public double observeDuration() {
            double elapsed = SimpleTimer.elapsedSecondsFromNanos(start, SimpleTimer.defaultTimeProvider.nanoTime());
            child.observe(elapsed);
            return elapsed;
        }

        /**
         * Equivalent to calling {@link #observeDuration()}.
         */
        @Override
        public void close() {
            observeDuration();
        }
    }

    /**
     * The value of a single Summary.
     * <p>
     * <em>Warning:</em> References to a Child become invalid after using
     * {@link SimpleCollector#remove} or {@link SimpleCollector#clear}.
     */
    public static class Child {

        /**
         * Executes runnable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
         *
         * @param timeable Code that is being timed
         * @return Measured duration in seconds for timeable to complete.
         */
        public double time(Runnable timeable) {
            Timer timer = startTimer();

            double elapsed;
            try {
                timeable.run();
            } finally {
                elapsed = timer.observeDuration();
            }
            return elapsed;
        }

        /**
         * Executes callable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
         *
         * @param timeable Code that is being timed
         * @return Result returned by callable.
         */
        public <E> E time(Callable<E> timeable) {
            Timer timer = startTimer();

            try {
                return timeable.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                timer.observeDuration();
            }
        }

        public static class Value {
            public final double count;
            public final double sum;
            public final SortedMap<Double, Double> quantiles;

            private Value(double count, double sum, List<Quantile> quantiles, TimeWindowQuantiles quantileValues) {
                this.count = count;
                this.sum = sum;
                this.quantiles = Collections.unmodifiableSortedMap(snapshot(quantiles, quantileValues));
            }

            private SortedMap<Double, Double> snapshot(List<Quantile> quantiles, TimeWindowQuantiles quantileValues) {
                SortedMap<Double, Double> result = new TreeMap<Double, Double>();
                for (Quantile q : quantiles) {
                    result.put(q.quantile, quantileValues.get(q.quantile));
                }
                return result;
            }
        }

        // Having these separate leaves us open to races,
        // however Prometheus as whole has other races
        // that mean adding atomicity here wouldn't be useful.
        // This should be reevaluated in the future.
        private final DoubleAdder count = new DoubleAdder();
        private final DoubleAdder sum = new DoubleAdder();
        private final List<Quantile> quantiles;
        private final TimeWindowQuantiles quantileValues;

        private Child(List<Quantile> quantiles, long maxAgeSeconds, int ageBuckets) {
            this.quantiles = quantiles;
            if (quantiles.size() > 0) {
                quantileValues = new TimeWindowQuantiles(quantiles.toArray(new Quantile[]{}), maxAgeSeconds, ageBuckets);
            } else {
                quantileValues = null;
            }
        }

        /**
         * Observe the given amount.
         */
        public void observe(double amt) {
            count.add(1);
            sum.add(amt);
            if (quantileValues != null) {
                quantileValues.insert(amt);
            }
        }

        /**
         * Start a timer to track a duration.
         * <p>
         * Call {@link Timer#observeDuration} at the end of what you want to measure the duration of.
         */
        public Timer startTimer() {
            return new Timer(this, SimpleTimer.defaultTimeProvider.nanoTime());
        }

        /**
         * Get the value of the Summary.
         * <p>
         * <em>Warning:</em> The definition of {@link Value} is subject to change.
         */
        public Value get() {
            return new Value(count.sum(), sum.sum(), quantiles, quantileValues);
        }
    }

    // Convenience methods.

    /**
     * Observe the given amount on the summary with no labels.
     */
    public void observe(double amt) {
        noLabelsChild.observe(amt);
    }

    /**
     * Start a timer to track a duration on the summary with no labels.
     * <p>
     * Call {@link Timer#observeDuration} at the end of what you want to measure the duration of.
     */
    public Timer startTimer() {
        return noLabelsChild.startTimer();
    }

    /**
     * Executes runnable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
     *
     * @param timeable Code that is being timed
     * @return Measured duration in seconds for timeable to complete.
     */
    public double time(Runnable timeable) {
        return noLabelsChild.time(timeable);
    }

    /**
     * Executes callable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
     *
     * @param timeable Code that is being timed
     * @return Result returned by callable.
     */
    public <E> E time(Callable<E> timeable) {
        return noLabelsChild.time(timeable);
    }

    /**
     * Get the value of the Summary.
     * <p>
     * <em>Warning:</em> The definition of {@link Child.Value} is subject to change.
     */
    public Child.Value get() {
        return noLabelsChild.get();
    }

    @Override
    public Collection<Series> collect() {
        List<Series> series = new ArrayList<Series>(children.size());

        Series.Builder builder = Series.newBuilder();
        for (Map.Entry<List<String>, Child> c : children.entrySet()) {
            Child.Value v = c.getValue().get();

            List<String> labelValues = c.getKey();
            for (int i = 0; i < labelNames.size(); i++) {
                builder.addLabel(labelNames.get(i), labelValues.get(i));
            }
            Point.Builder pointBuilder = builder.addPointBuilder();

            builder.setMetricName(metricName + "_count");
            pointBuilder.setT(System.currentTimeMillis()).setV(v.count);
            series.add(builder.build());

            builder.setMetricName(metricName + "_sum");
            pointBuilder.setT(System.currentTimeMillis()).setV(v.sum);
            series.add(builder.build());

            builder.setMetricName(metricName);
            Label.Builder labelBuilder = builder.addLabelBuilder().setName("quantile");
            for (Map.Entry<Double, Double> q : v.quantiles.entrySet()) {
                labelBuilder.setValue(doubleToGoString(q.getKey()));
                pointBuilder.setT(System.currentTimeMillis()).setV(q.getValue());

                series.add(builder.build());
            }
        }

        return series;
    }
}
