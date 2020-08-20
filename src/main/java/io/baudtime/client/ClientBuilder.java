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

import io.baudtime.client.netty.*;
import io.baudtime.discovery.ServiceAddrProvider;
import io.baudtime.message.BaudMessage;
import io.baudtime.message.GeneralResponse;

import java.util.HashMap;
import java.util.Map;

public abstract class ClientBuilder<B, C extends Client> {

    protected FutureListener futureListener;
    protected KeyBoundClient.KeyMapping keyMapping;
    protected ClientConfig.Builder configBuilder = new ClientConfig.Builder();

    public B writeResponseHook(FutureListener futureListener) {
        this.futureListener = futureListener;
        return thisBuilder();
    }

    public B keyMapping(KeyBoundClient.KeyMapping<? extends Comparable> keyMapping) {
        this.keyMapping = keyMapping;
        return thisBuilder();
    }

    /*
     * @deprecated Use FutureListener instead
     */
    @Deprecated
    public B writeResponseHook(final WriteResponseHook hook) {
        this.futureListener = new FutureListener() {
            private WriteResponseHook responseHook = hook;

            @Override
            public void onFinished(Future future) {
                BaudMessage response = future.getResponse();
                if (future.isSendRequestOK() && response instanceof GeneralResponse) {
                    this.responseHook.onReceiveResponse(future.getOpaque(), (GeneralResponse) response);
                }
            }
        };
        return thisBuilder();
    }

    public B connectTimeoutMillis(int connectTimeoutMillis) {
        this.configBuilder.connectTimeoutMillis(connectTimeoutMillis);
        return thisBuilder();
    }

    public B socketSndBufSize(int socketSndBufSize) {
        this.configBuilder.connectTimeoutMillis(socketSndBufSize);
        return thisBuilder();
    }

    public B socketRcvBufSize(int socketRcvBufSize) {
        this.configBuilder.socketRcvBufSize(socketRcvBufSize);
        return thisBuilder();
    }

    public B writeBufLowWaterMark(int writeBufLowWaterMark) {
        this.configBuilder.writeBufLowWaterMark(writeBufLowWaterMark);
        return thisBuilder();
    }

    public B writeBufHighWaterMark(int writeBufHighWaterMark) {
        this.configBuilder.writeBufHighWaterMark(writeBufHighWaterMark);
        return thisBuilder();
    }

    public B writeFlowControlLimit(int writeFlowControlLimit) {
        this.configBuilder.writeFlowControlLimit(writeFlowControlLimit);
        return thisBuilder();
    }

    public B readFlowControlLimit(int readFlowControlLimit) {
        this.configBuilder.readFlowControlLimit(readFlowControlLimit);
        return thisBuilder();
    }

    public B maxResponseFrameLength(int maxResponseFrameLength) {
        this.configBuilder.maxResponseFrameLength(maxResponseFrameLength);
        return thisBuilder();
    }

    public B maxConnectionsOnEachServer(int maxConnectionsOnEachServer) {
        this.configBuilder.maxConnectionsOnEachServer(maxConnectionsOnEachServer);
        return thisBuilder();
    }

    public B flushChannelOnEachWrite(boolean flushChannelOnEachWrite) {
        this.configBuilder.flushChannelOnEachWrite(flushChannelOnEachWrite);
        return thisBuilder();
    }

    public B channelMaxIdleTimeSeconds(int channelMaxIdleTimeSeconds) {
        this.configBuilder.channelMaxIdleTimeSeconds(channelMaxIdleTimeSeconds);
        return thisBuilder();
    }

    public B stickyWorkerNum(int workerNum) {
        this.configBuilder.stickyWorkerNum(workerNum);
        return thisBuilder();
    }

    public B stickyBatchSize(int batchSize) {
        this.configBuilder.stickyBatchSize(batchSize);
        return thisBuilder();
    }

    @Deprecated
    public B stickyQueueCapacity(int queueCapacity) {
        return thisBuilder();
    }

    public abstract C build();

    public abstract B thisBuilder();

    public <Records> RecordsAdaptor<C, Records> build(RecordsAdaptor.RecordsConverter<Records> recordsConverter) {
        return RecordsAdaptor.wrap(build(), recordsConverter);
    }

    public static class SingleEndpointClientBuilder extends ClientBuilder<SingleEndpointClientBuilder, BaudClient> {
        private ServiceAddrProvider serviceAddrProvider;

        public SingleEndpointClientBuilder serviceAddrProvider(ServiceAddrProvider serviceAddrProvider) {
            this.serviceAddrProvider = serviceAddrProvider;
            return this;
        }

        @Override
        public BaudClient build() {
            if (serviceAddrProvider == null) {
                throw new RuntimeException("serviceAddrProvider must be provided");
            }
            ClientConfig clientConfig = configBuilder.build();

            TcpClient tcpClient = null;

            if (keyMapping != null && clientConfig.getStickyConfig() != null) {
                throw new RuntimeException("must not set key mapping and sticky config at the same time");
            }

            if (keyMapping == null && clientConfig.getStickyConfig() == null) {
                tcpClient = new RoundRobinClient(clientConfig, serviceAddrProvider, futureListener);
            }

            if (keyMapping == null && clientConfig.getStickyConfig() != null) {
                tcpClient = new StickyClient(clientConfig, serviceAddrProvider, futureListener);
            }

            if (keyMapping != null && clientConfig.getStickyConfig() == null) {
                tcpClient = new KeyBoundClient(keyMapping, clientConfig, serviceAddrProvider, futureListener);
            }

            return new BaudClient(tcpClient);
        }

        @Override
        public SingleEndpointClientBuilder thisBuilder() {
            return this;
        }
    }

    public static class MultiEndpointClientBuilder extends ClientBuilder<MultiEndpointClientBuilder, MultiEndpointClient> {
        private Map<String, ServiceAddrProvider> multiEndpointAddrProviders = new HashMap<String, ServiceAddrProvider>();

        public MultiEndpointClientBuilder serviceAddrProvider(String endpoint, ServiceAddrProvider serviceAddrProvider) {
            this.multiEndpointAddrProviders.put(endpoint, serviceAddrProvider);
            return this;
        }

        public MultiEndpointClient build() {
            if (multiEndpointAddrProviders.isEmpty()) {
                throw new RuntimeException("serviceAddrProvider must be provided");
            }

            ClientConfig clientConfig = configBuilder.build();

            if (keyMapping != null && clientConfig.getStickyConfig() != null) {
                throw new RuntimeException("must not set key mapping and sticky config at the same time");
            }

            MultiEndpointClient multiEndpointClient = new MultiEndpointClient();
            for (Map.Entry<String, ServiceAddrProvider> e : multiEndpointAddrProviders.entrySet()) {
                String endPoint = e.getKey();
                ServiceAddrProvider serviceAddrProvider = e.getValue();

                TcpClient tcpClient = null;

                if (keyMapping == null && clientConfig.getStickyConfig() == null) {
                    tcpClient = new RoundRobinClient(clientConfig, serviceAddrProvider, futureListener);
                }

                if (keyMapping == null && clientConfig.getStickyConfig() != null) {
                    tcpClient = new StickyClient(clientConfig, serviceAddrProvider, futureListener);
                }

                if (keyMapping != null && clientConfig.getStickyConfig() == null) {
                    tcpClient = new KeyBoundClient(keyMapping, clientConfig, serviceAddrProvider, futureListener);
                }

                multiEndpointClient.addEndpoint(endPoint, new BaudClient(tcpClient));
            }
            return multiEndpointClient;
        }

        @Override
        public MultiEndpointClientBuilder thisBuilder() {
            return this;
        }
    }

    public static SingleEndpointClientBuilder newClientBuilder() {
        return new SingleEndpointClientBuilder();
    }

    public static MultiEndpointClientBuilder newMultiEndpointClientBuilder() {
        return new MultiEndpointClientBuilder();
    }
}
