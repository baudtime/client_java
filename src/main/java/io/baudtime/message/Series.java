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

package io.baudtime.message;

import io.baudtime.util.Assert;
import net.openhft.hashing.LongHashFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.baudtime.message.MessageCheck.checkLabelName;

public class Series implements Hashed {
    private final List<Label> labels;
    private final List<Point> points;

    private final int hashcode;

    public Series(List<Label> labels, List<Point> points) {
        Collections.sort(labels, Label.comparator);
        Collections.sort(points, Point.comparator);

        this.labels = labels;
        this.points = points;

        StringBuilder sb = new StringBuilder();
        for (Label lb : labels) {
            sb.append(lb.getName());
            sb.append(lb.getValue());
        }
        this.hashcode = (int) (LongHashFunction.xx().hashChars(sb));
    }

    public List<Label> getLabels() {
        return Collections.unmodifiableList(labels);
    }

    public List<Point> getPoints() {
        return Collections.unmodifiableList(points);
    }

    public int hash() {
        return hashcode;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (Label l : labels) {
            sb.append(l.getName()).append("=").append(l.getValue());
        }
        sb.append('}');

        for (Point p : points) {
            sb.append(" ").append(p.T()).append(",").append(p.V());
        }

        return sb.toString();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private static final String __metricName__ = "__name__";

        private Label.Builder metricNameBuilder = null;
        private final List<Label> labels = new ArrayList<Label>();
        private final List<Point> points = new ArrayList<Point>();
        private final List<Label.Builder> labelBuilders = new ArrayList<Label.Builder>();
        private final List<Point.Builder> pointBuilders = new ArrayList<Point.Builder>();

        public Builder setMetricName(String name) {
            if (metricNameBuilder == null) {
                metricNameBuilder = Label.newBuilder();
            }
            checkLabelName(name);
            metricNameBuilder.setName(__metricName__).setValue(name);
            return this;
        }

        @Deprecated
        public Builder setName(String name) {
            checkLabelName(name);
            return setMetricName(name);
        }

        public Builder addLabel(String name, String value) {
            checkLabelName(name);
            labels.add(new Label(name, value));
            return this;
        }

        public Builder addLabels(List<Label> labels) {
            for (Label lb : labels) {
                checkLabelName(lb.getName());
            }
            this.labels.addAll(labels);
            return this;
        }

        public Builder addPoint(long t, double v) {
            points.add(new Point(t, v));
            return this;
        }

        public Builder addPoints(List<Point> points) {
            this.points.addAll(points);
            return this;
        }

        public Label.Builder addLabelBuilder() {
            Label.Builder lb = Label.newBuilder();
            labelBuilders.add(lb);
            return lb;
        }

        public Point.Builder addPointBuilder() {
            Point.Builder pb = Point.newBuilder();
            pointBuilders.add(pb);
            return pb;
        }

        public Series build() {
            Assert.notNull(metricNameBuilder, "metric name must be set");

            List<Label> ls = new ArrayList<Label>(labels.size() + labelBuilders.size() + 1);
            ls.add(metricNameBuilder.build());

            for (Label lb : labels) {
                String name = lb.getName();
                String value = lb.getValue();
                if (name != null && name.length() > 0 && value != null && value.length() > 0) {
                    ls.add(lb);
                }
            }
            for (Label.Builder lb : labelBuilders) {
                ls.add(lb.build());
            }

            List<Point> ps = new ArrayList<Point>(points.size() + pointBuilders.size());
            if (points.size() > 0) {
                ps.addAll(points);
            }
            for (Point.Builder pb : pointBuilders) {
                ps.add(pb.build());
            }

            Assert.isPositive(ps.size(), "series has no points");

            return new Series(ls, ps);
        }

        Series fastBuild() {
            return new Series(labels, points);
        }

        public void clear() {
            labels.clear();
            points.clear();
            labelBuilders.clear();
            pointBuilders.clear();
        }
    }
}
