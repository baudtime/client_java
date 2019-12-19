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

public class ClientConfig {

    protected int connectTimeoutMillis = 3000;
    protected int socketSndBufSize = 65535;
    protected int socketRcvBufSize = 65535;
    protected int writeBufLowWaterMark = 32 * 1024;
    protected int writeBufHighWaterMark = 256 * 1024;
    protected int maxResponseFrameLength = 150 * 1024 * 1024;
    protected boolean flushChannelOnEachWrite = false;

    protected long channelNotActiveInterval = 1000 * 60;
    protected int channelMaxIdleTimeSeconds = 120;
    protected boolean closeSocketIfTimeout = false;

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public int getSocketSndBufSize() {
        return socketSndBufSize;
    }

    public int getSocketRcvBufSize() {
        return socketRcvBufSize;
    }

    public int getWriteBufLowWaterMark() {
        return writeBufLowWaterMark;
    }

    public int getWriteBufHighWaterMark() {
        return writeBufHighWaterMark;
    }

    public int getMaxResponseFrameLength() {
        return maxResponseFrameLength;
    }

    public boolean isFlushChannelOnEachWrite() {
        return flushChannelOnEachWrite;
    }

    public long getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }

    public int getChannelMaxIdleTimeSeconds() {
        return channelMaxIdleTimeSeconds;
    }

    public boolean isCloseSocketIfTimeout() {
        return closeSocketIfTimeout;
    }
}
