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

import io.baudtime.client.ClientConfig;
import io.baudtime.discovery.ServiceAddrProvider;
import io.baudtime.message.AddRequest;
import io.baudtime.message.Series;
import io.netty.channel.Channel;

import java.util.Collection;

public class RoundRobinClient extends AbstractClient {

    public RoundRobinClient(ClientConfig clientConfig, ServiceAddrProvider serviceAddrProvider, FutureListener writeResponseHook) {
        super(clientConfig, serviceAddrProvider, writeResponseHook);
    }

    @Override
    public void append(Collection<Series> series) {
        AddRequest.Builder reqBuilder = AddRequest.newBuilder();
        reqBuilder.addSeries(series);

        Channel ch = null;
        try {
            ch = getChannel();
            asyncRequest(ch, reqBuilder.build());
        } finally {
            putChannel(ch);
        }
    }
}