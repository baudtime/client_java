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
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


public class AddRequest implements BaudMessage {
    private Collection<Series> series;

    private AddRequest(Collection<Series> series) {
        this.series = series;
    }

    public byte[] marshal() {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packArrayHeader(1);

            packer.packArrayHeader(series.size());
            for (Series s : series) {
                packer.packArrayHeader(2);

                List<Label> labels = s.getLabels();
                packer.packArrayHeader(labels.size());
                for (Label l : labels) {
                    packer.packArrayHeader(2);
                    packer.packString(l.getName());
                    packer.packString(l.getValue());
                }

                List<Point> points = s.getPoints();
                packer.packArrayHeader(points.size());
                for (Point p : points) {
                    packer.packArrayHeader(2);
                    packer.packLong(p.T());
                    packer.packDouble(p.V());
                }
            }
        } catch (IOException e) {
            throw new Exceptions.MarshalException(e);
        } finally {
            try {
                packer.close();
            } catch (IOException e) {
                //
            }
        }

        return packer.toByteArray();
    }

    public void unmarshal(ByteBuffer b) {
        series = new ArrayList<Series>();

        MessageUnpacker unPacker = MessagePack.newDefaultUnpacker(b);
        try {
            unPacker.unpackArrayHeader();//1

            int size = unPacker.unpackArrayHeader();
            for (int i = 0; i < size; i++) {
                Assert.equal(unPacker.unpackArrayHeader(), 2);

                int sz = unPacker.unpackArrayHeader();
                List<Label> labels = new ArrayList<Label>(sz);
                for (int j = 0; j < sz; j++) {
                    Assert.equal(unPacker.unpackArrayHeader(), 2);

                    String label = unPacker.unpackString();
                    String name = unPacker.unpackString();
                    labels.add(new Label(label, name));
                }

                sz = unPacker.unpackArrayHeader();
                List<Point> points = new ArrayList<Point>(sz);
                for (int j = 0; j < sz; j++) {
                    Assert.equal(unPacker.unpackArrayHeader(), 2);

                    long t = unPacker.unpackLong();
                    double v = unPacker.unpackDouble();
                    points.add(new Point(t, v));
                }

                series.add(new Series(labels, points));
            }

        } catch (IOException e) {
            throw new Exceptions.UnmarshalException(e);
        } finally {
            try {
                unPacker.close();
            } catch (IOException e) {
                //
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder().withMerge();
    }

    public static class Builder {

        private List<Series> toBuild = new LinkedList<Series>();
        private int size;
        private boolean withMerge;

        public Builder addSeries(Series series) {
            toBuild.add(series);
            size += series.getPoints().size();

            return this;
        }

        public Builder addSeries(Iterable<Series> series) {
            for (Series s : series) {
                addSeries(s);
            }

            return this;
        }

        public Builder withMerge() {
            this.withMerge = true;
            return this;
        }

        public int size() {
            return size;
        }

        public AddRequest build() {
            if (!withMerge) {
                return new AddRequest(toBuild);
            }

            Map<String, Series.Builder> buf = new HashMap<String, Series.Builder>();
            for (Series series : toBuild) {
                List<Label> lbls = series.getLabels();

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < lbls.size(); i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(lbls.get(i).getName());
                    sb.append('=');
                    sb.append(lbls.get(i).getValue());
                }
                String k = sb.toString();

                Series.Builder seriesBuilder = buf.get(k);
                if (seriesBuilder == null) {
                    seriesBuilder = Series.newBuilder().noLabelNameCheck();
                    seriesBuilder.addLabels(lbls);
                    buf.put(k, seriesBuilder);
                }
                seriesBuilder.addPoints(series.getPoints());
            }

            try {
                ArrayList<Series> series = new ArrayList<Series>();

                for (Map.Entry<String, Series.Builder> e : buf.entrySet()) {
                    series.add(e.getValue().fastBuild());
                }

                return new AddRequest(series);
            } finally {
                clear();
            }
        }

        public void clear() {
            toBuild = new LinkedList<Series>();
            size = 0;
        }
    }

    public static class MergedBuilder {
        private final List<Builder> builders = new LinkedList<Builder>();
        private int size;

        public MergedBuilder merge(Builder builder) {
            builders.add(builder);
            size += builder.size();
            return this;
        }

        public int size() {
            return size;
        }

        public AddRequest build() {
            Map<String, Series.Builder> buf = new HashMap<String, Series.Builder>();
            for (Builder builder : builders) {
                for (Series series : builder.toBuild) {
                    List<Label> lbls = series.getLabels();

                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < lbls.size(); i++) {
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append(lbls.get(i).getName());
                        sb.append('=');
                        sb.append(lbls.get(i).getValue());
                    }
                    String k = sb.toString();

                    Series.Builder seriesBuilder = buf.get(k);
                    if (seriesBuilder == null) {
                        seriesBuilder = Series.newBuilder();
                        seriesBuilder.addLabels(lbls);
                        buf.put(k, seriesBuilder);
                    }
                    seriesBuilder.addPoints(series.getPoints());
                }
            }

            try {
                ArrayList<Series> series = new ArrayList<Series>();

                for (Map.Entry<String, Series.Builder> e : buf.entrySet()) {
                    series.add(e.getValue().fastBuild());
                }

                return new AddRequest(series);
            } finally {
                clear();
            }
        }

        public void clear() {
            builders.clear();
            size = 0;
        }
    }
}
