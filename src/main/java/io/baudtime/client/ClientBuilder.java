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

import io.baudtime.discovery.ServiceAddrProvider;

public class ClientBuilder extends ClientConfig {
    private ServiceAddrProvider serviceAddrProvider;
    private WriteResponseHook writeResponseHook;

    public ClientBuilder serviceAddrProvider(ServiceAddrProvider serviceAddrProvider) {
        this.serviceAddrProvider = serviceAddrProvider;
        return this;
    }

    public ClientBuilder writeResponseHook(WriteResponseHook writeResponseHook) {
        this.writeResponseHook = writeResponseHook;
        return this;
    }

    public ClientBuilder connectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public ClientBuilder socketSndBufSize(int socketSndBufSize) {
        this.socketSndBufSize = socketSndBufSize;
        return this;
    }

    public ClientBuilder socketRcvBufSize(int socketRcvBufSize) {
        this.socketRcvBufSize = socketRcvBufSize;
        return this;
    }

    public ClientBuilder writeBufLowWaterMark(int writeBufLowWaterMark) {
        this.writeBufLowWaterMark = writeBufLowWaterMark;
        return this;
    }

    public ClientBuilder writeBufHighWaterMark(int writeBufHighWaterMark) {
        this.writeBufHighWaterMark = writeBufHighWaterMark;
        return this;
    }

    public ClientBuilder maxResponseFrameLength(int maxResponseFrameLength) {
        this.maxResponseFrameLength = maxResponseFrameLength;
        return this;
    }

    public ClientBuilder flushChannelOnEachWrite(boolean flushChannelOnEachWrite) {
        this.flushChannelOnEachWrite = flushChannelOnEachWrite;
        return this;
    }

    public ClientBuilder channelNotActiveInterval(long channelNotActiveInterval) {
        this.channelNotActiveInterval = channelNotActiveInterval;
        return this;
    }

    public ClientBuilder channelMaxIdleTimeSeconds(int channelMaxIdleTimeSeconds) {
        this.channelMaxIdleTimeSeconds = channelMaxIdleTimeSeconds;
        return this;
    }

    public ClientBuilder closeSocketIfTimeout(boolean closeSocketIfTimeout) {
        this.closeSocketIfTimeout = closeSocketIfTimeout;
        return this;
    }

    public Client build() {
        if (serviceAddrProvider == null) {
            throw new RuntimeException("serviceAddrProvider must be set");
        }
        return new BaudClient(this, serviceAddrProvider, writeResponseHook);
    }
}