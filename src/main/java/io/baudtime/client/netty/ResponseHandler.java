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

import io.baudtime.message.BaudMessage;
import io.baudtime.message.GeneralResponse;
import io.baudtime.message.StatusCode;
import io.baudtime.util.ConcurrentReferenceHashMap;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static io.baudtime.util.ConcurrentReferenceHashMap.ReferenceType;

@Sharable
class ResponseHandler extends SimpleChannelInboundHandler<Message> {

    private final ConcurrentReferenceHashMap<Long, Future> futures = new ConcurrentReferenceHashMap<Long, Future>(16, ReferenceType.STRONG, ReferenceType.SOFT);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        Future future = futures.get(msg.getOpaque());
        if (future != null) {
            try {
                BaudMessage response = msg.getRaw();
                if (response instanceof GeneralResponse && StatusCode.Failed == ((GeneralResponse) response).getStatus()) {
                    future.setCause(new Exception(((GeneralResponse) response).getMessage()));
                }
                future.setResponse(response).finish();
            } finally {
                futures.remove(future.getOpaque());
            }
        }
    }

    public void registerFuture(Future future) {
        if (future != null) {
            futures.put(future.getOpaque(), future);
        }
    }

    public void releaseFuture(Future future) {
        if (future != null) {
            futures.remove(future.getOpaque());
        }
    }
}
