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

public class InstantQueryRequest implements BaudMessage {
    private String time;
    private String timeout;
    private String query;

    private InstantQueryRequest() {
    }

    public byte[] marshal() {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packMapHeader(3);

            packer.packString("time");
            packer.packString(time);

            packer.packString("timeout");
            packer.packString(timeout);

            packer.packString("query");
            packer.packString(query);
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
                if (key.equals("time")) {
                    time = unPacker.unpackString();
                } else if (key.equals("timeout")) {
                    timeout = unPacker.unpackString();
                } else if (key.equals("query")) {
                    query = unPacker.unpackString();
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
        private String time;
        private String timeout;
        private String query;

        public Builder setTime(Date time) {
            this.time = Formatter.format(time);
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

        public InstantQueryRequest build() {
            Assert.notEmpty(this.timeout);
            Assert.notEmpty(this.query);

            InstantQueryRequest r = new InstantQueryRequest();
            r.time = this.time == null ? "" : this.time;
            r.timeout = this.timeout;
            r.query = this.query;
            return r;
        }
    }
}
