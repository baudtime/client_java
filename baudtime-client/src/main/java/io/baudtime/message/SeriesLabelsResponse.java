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
import java.util.ArrayList;
import java.util.List;

public class SeriesLabelsResponse implements BaudMessage {
    private List<List<Label>> labels;
    private StatusCode status;
    private String errorMsg;

    public List<List<Label>> getLabels() {
        return labels;
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

            packer.packString("labels");
            packer.packArrayHeader(labels.size());
            for (List<Label> labels : labels) {
                packer.packArrayHeader(labels.size());
                for (Label l : labels) {
                    packer.packArrayHeader(2);
                    packer.packString(l.getName());
                    packer.packString(l.getValue());
                }
            }

            packer.packString("status");
            packer.packByte(status.value());

            packer.packString("errorMsg");
            packer.packString(errorMsg);
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
        labels = new ArrayList<List<Label>>();

        MessageUnpacker unPacker = MessagePack.newDefaultUnpacker(b);
        try {
            int size = unPacker.unpackMapHeader();
            for (int i = 0; i < size; i++) {
                String key = unPacker.unpackString();
                if (key.equals("labels")) {
                    int sz = unPacker.unpackArrayHeader();
                    for (int j = 0; j < sz; j++) {
                        List<Label> labels = new ArrayList<Label>();
                        int lbNum = unPacker.unpackArrayHeader();
                        for (int k = 0; k < lbNum; k++) {
                            Assert.equal(unPacker.unpackArrayHeader(), 2);

                            String label = unPacker.unpackString();
                            String name = unPacker.unpackString();
                            labels.add(new Label(label, name));
                        }
                        this.labels.add(labels);
                    }
                } else if (key.equals("status")) {
                    status = StatusCode.parse(unPacker.unpackByte());
                } else if (key.equals("errorMsg")) {
                    errorMsg = unPacker.unpackString();
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


}
