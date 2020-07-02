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
import io.baudtime.util.Util.Formatter;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;


public class RangeQueryRequest implements BaudMessage {
    private String start;
    private String end;
    private String step;
    private String timeout;
    private String query;

    private RangeQueryRequest() {
    }

    public byte[] marshal() {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packMapHeader(5);

            packer.packString("start");
            packer.packString(start);

            packer.packString("end");
            packer.packString(end);

            packer.packString("step");
            packer.packString(step);

            packer.packString("timeout");
            packer.packString(timeout);

            packer.packString("query");
            packer.packString(query);
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
        MessageUnpacker unPacker = MessagePack.newDefaultUnpacker(b);
        try {
            int size = unPacker.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                String key = unPacker.unpackString();
                if (key.equals("start")) {
                    start = unPacker.unpackString();
                } else if (key.equals("end")) {
                    end = unPacker.unpackString();
                } else if (key.equals("step")) {
                    step = unPacker.unpackString();
                } else if (key.equals("timeout")) {
                    timeout = unPacker.unpackString();
                } else if (key.equals("query")) {
                    query = unPacker.unpackString();
                } else {
                    throw new Exceptions.UnmarshalException("unexpect key");
                }
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
        return new Builder();
    }

    public static class Builder {
        private String start;
        private String end;
        private String step;
        private String timeout;
        private String query;

        public Builder setStart(Date start) {
            this.start = Formatter.format(start);
            return this;
        }

        public Builder setEnd(Date end) {
            this.end = Formatter.format(end);
            return this;
        }

        public Builder setStep(String step) {
            this.step = step;
            return this;
        }

        public Builder setTimeout(String timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        public RangeQueryRequest build() {
            Assert.notEmpty(this.start);
            Assert.notEmpty(this.end);
            Assert.notEmpty(this.step);
            Assert.notEmpty(this.timeout);
            Assert.notEmpty(this.query);

            RangeQueryRequest r = new RangeQueryRequest();
            r.start = this.start;
            r.end = this.end;
            r.step = this.step;
            r.timeout = this.timeout;
            r.query = this.query;
            return r;
        }
    }
}
