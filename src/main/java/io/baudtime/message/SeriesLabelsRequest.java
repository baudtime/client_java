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
import io.baudtime.util.Util;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

public class SeriesLabelsRequest implements BaudMessage {
    private Collection<String> matches;
    private String start;
    private String end;
    private String timeout;

    private SeriesLabelsRequest() {
    }

    public byte[] marshal() {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packMapHeader(4);

            packer.packString("start");
            packer.packString(start);

            packer.packString("end");
            packer.packString(end);

            packer.packString("matches");
            packer.packArrayHeader(matches.size());
            for (String match : matches) {
                packer.packString(match);
            }

            packer.packString("timeout");
            packer.packString(timeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        MessageUnpacker unPacker = MessagePack.newDefaultUnpacker(b);
        try {
            int size = unPacker.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                String key = unPacker.unpackString();
                if (key.equals("start")) {
                    start = unPacker.unpackString();
                } else if (key.equals("end")) {
                    end = unPacker.unpackString();
                } else if (key.equals("timeout")) {
                    timeout = unPacker.unpackString();
                } else if (key.equals("matches")) {
                    int sz = unPacker.unpackArrayHeader();
                    matches = new ArrayList<String>(sz);
                    for (int j = 0; j < sz; j++) {
                        matches.add(unPacker.unpackString());
                    }
                } else {
                    throw new RuntimeException("unexpect key");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                unPacker.close();
            } catch (IOException e) {
                //
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Collection<String> matches;
        private String start;
        private String end;
        private String timeout;


        public Builder setStart(Date start) {
            this.start = Util.Formatter.format(start);
            return this;
        }

        public Builder setEnd(Date end) {
            this.end = Util.Formatter.format(end);
            return this;
        }

        public Builder setTimeout(String timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setMatches(Collection<String> matches) {
            this.matches = matches;
            return this;
        }

        public SeriesLabelsRequest build() {
            Assert.notEmpty(this.start);
            Assert.notEmpty(this.end);
            Assert.notEmpty(this.timeout);
            Assert.isNotNegative(this.matches.size());

            SeriesLabelsRequest r = new SeriesLabelsRequest();
            r.start = this.start;
            r.end = this.end;
            r.timeout = this.timeout;
            r.matches = this.matches;
            return r;
        }
    }
}
