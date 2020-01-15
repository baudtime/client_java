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

package io.baudtime.client.netty;

import io.baudtime.message.*;
import io.baudtime.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RequestEncoder extends MessageToByteEncoder<Message> {
    private static final Logger log = LoggerFactory.getLogger(RequestEncoder.class);

    @Override
    public void encode(ChannelHandlerContext ctx, Message request, ByteBuf out) {
        try {
            byte[] opaqueBytes = Util.varLongToBytes(request.getOpaque());

            BaudMessage raw = request.getRaw();
            byte[] rawBytes = raw.marshal();

            //1. write length(4 bytes)
            out.writeBytes(Util.intToFixedLengthBytes(1 + opaqueBytes.length + rawBytes.length));

            //2. write type(1 byte)
            if (raw instanceof AddRequest) {
                out.writeByte(0);
            } else if (raw instanceof InstantQueryRequest) {
                out.writeByte(1);
            } else if (raw instanceof RangeQueryRequest) {
                out.writeByte(2);
            } else if (raw instanceof SeriesLabelsRequest) {
                out.writeByte(4);
            } else if (raw instanceof LabelValuesRequest) {
                out.writeByte(6);
            } else {
                throw new RuntimeException("bad request format");
            }

            //3. write opaque(opaqueBytes.length bytes)
            out.writeBytes(opaqueBytes);

            //4. write raw msg(rawBytes.length bytes)
            out.writeBytes(rawBytes);
        } catch (Exception e) {
            log.error("encode exception, " + ctx.channel().remoteAddress(), e);
            ctx.close();
        }
    }
}

