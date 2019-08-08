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

package io.baudtime.client;

import io.baudtime.message.BaudMessage;
import io.baudtime.message.GeneralResponse;
import io.baudtime.message.LabelValuesResponse;
import io.baudtime.message.QueryResponse;
import io.baudtime.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResponseDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log = LoggerFactory.getLogger(ResponseDecoder.class);

    public ResponseDecoder(int maxFrameLength) {
        super(maxFrameLength, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        ByteBuf raw = null;

        try {
            if (in.readableBytes() >= 4) {
                frame = (ByteBuf) super.decode(ctx, in);
                if (null == frame) {
                    return null;
                }

                int type = frame.readByte();
                long opaque = Util.readVarLong(frame);

                BaudMessage resp = null;
                raw = frame.readBytes(frame.readableBytes());
                if (type == 3) {
                    resp = new QueryResponse();
                } else if (type == 17) {
                    resp = new GeneralResponse();
                } else if (type == 18) {
                    resp = new LabelValuesResponse();
                }

                if (resp != null) {
                    resp.unmarshal(raw.nioBuffer());
                    return new Message(opaque, resp);
                } else {
                    throw new RuntimeException("unknown type");
                }
            }
        } catch (Exception e) {
            log.error("decode exception, " + ctx.channel().remoteAddress(), e);
            throw e;
        } finally {
            if (null != raw) {
                raw.release();
            }
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }
}

