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

import java.util.Comparator;
import java.util.Date;

public class Point {
    private final long t;
    private final double v;

    public Point(long t, double v) {
        this.t = t;
        this.v = v;
    }

    public long T() {
        return t;
    }

    public double V() {
        return v;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long t;
        private double v;

        public Builder setT(long t) {
            this.t = t;
            return this;
        }

        public Builder setT(Date t) {
            this.t = t.getTime();
            return this;
        }

        public Builder setV(double v) {
            this.v = v;
            return this;
        }

        public Point build() {
            Assert.isPositive(this.t);

            return new Point(this.t, this.v);
        }
    }

    public static final Comparator<Point> comparator = new Comparator<Point>() {
        @Override
        public int compare(Point p1, Point p2) {
            if (p1.T() < p2.T()) {
                return -1;
            } else if (p1.T() > p2.T()) {
                return 1;
            }
            return 0;
        }
    };
}
