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
import java.util.Date;

public class LabelValuesRequest implements BaudMessage {
    private String name;
    private String start;
    private String end;
    private String timeout;

    private LabelValuesRequest() {
    }

    public byte[] marshal() {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packMapHeader(4);

            packer.packString("name");
            packer.packString(name);

            packer.packString("start");
            packer.packString(start);

            packer.packString("end");
            packer.packString(end);

            packer.packString("timeout");
            packer.packString(timeout);
        } catch (IOException e) {
            throw new MessageCheck.MarshalException(e);
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
                if (key.equals("name")) {
                    name = unPacker.unpackString();
                } else if (key.equals("start")) {
                    start = unPacker.unpackString();
                } else if (key.equals("end")) {
                    end = unPacker.unpackString();
                } else if (key.equals("timeout")) {
                    timeout = unPacker.unpackString();
                } else {
                    throw new MessageCheck.UnmarshalException("unexpect key");
                }
            }
        } catch (IOException e) {
            throw new MessageCheck.UnmarshalException(e);
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
        private String name;
        private String start;
        private String end;
        private String timeout;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setStart(Date start) {
            this.start = (start == null ? "" : Util.Formatter.format(start));
            return this;
        }

        public Builder setEnd(Date end) {
            this.end = (end == null ? "" : Util.Formatter.format(end));
            return this;
        }

        public Builder setTimeout(String timeout) {
            this.timeout = timeout;
            return this;
        }

        public LabelValuesRequest build() {
            Assert.notEmpty(this.name);
            Assert.notEmpty(this.timeout);

            LabelValuesRequest r = new LabelValuesRequest();
            r.name = this.name;
            r.start = this.start;
            r.end = this.end;
            r.timeout = this.timeout;
            return r;
        }
    }
}
