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
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.ConcurrentHashMap;


@Sharable
class ResponseHandler extends SimpleChannelInboundHandler<Message> {

    private final ConcurrentHashMap<Long, ResponseFuture> futures = new ConcurrentHashMap<Long, ResponseFuture>();
    private WriteResponseHook writeResponseHook;

    public ResponseHandler() {
    }

    public ResponseHandler(WriteResponseHook writeResponseHook) {
        this.writeResponseHook = writeResponseHook;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        BaudMessage raw = msg.getRaw();
        if (raw instanceof GeneralResponse) {
            if (this.writeResponseHook != null) {
                this.writeResponseHook.onReceiveResponse(msg.getOpaque(), (GeneralResponse) raw);
            }
        } else {
            ResponseFuture f = futures.get(msg.getOpaque());
            if (f != null) {
                f.putResponse(raw);
            }
        }
        ReferenceCountUtil.release(msg);
    }

    public void registerFuture(ResponseFuture future) {
        if (future != null) {
            futures.put(future.getOpaque(), future);
        }
    }

    public void releaseFuture(ResponseFuture future) {
        if (future != null) {
            futures.remove(future.getOpaque());
        }
    }
}
