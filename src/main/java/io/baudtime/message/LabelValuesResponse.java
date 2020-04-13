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

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LabelValuesResponse implements BaudMessage {
    private List<String> values;
    private StatusCode status;
    private String errorMsg;

    public List<String> getValues() {
        return values;
    }

    public StatusCode getStatus() {
        return status;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public byte[] marshal() {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packMapHeader(3);

            packer.packString("values");
            packer.packArrayHeader(values.size());
            for (String v : values) {
                packer.packString(v);
            }

            packer.packString("status");
            packer.packByte(status.value());

            packer.packString("errorMsg");
            packer.packString(errorMsg);
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
        values = new ArrayList<String>();

        MessageUnpacker unPacker = MessagePack.newDefaultUnpacker(b);
        try {
            int size = unPacker.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                String key = unPacker.unpackString();
                if (key.equals("values")) {
                    int sz = unPacker.unpackArrayHeader();
                    for (int j = 0; j < sz; j++) {
                        values.add(unPacker.unpackString());
                    }
                } else if (key.equals("status")) {
                    status = StatusCode.parse(unPacker.unpackByte());
                } else if (key.equals("errorMsg")) {
                    errorMsg = unPacker.unpackString();
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
}
