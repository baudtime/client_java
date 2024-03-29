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


import io.baudtime.util.Assert;

public class ClientConfig {

    private int connectTimeoutMillis;
    private int socketSndBufSize;
    private int socketRcvBufSize;
    private int writeBufLowWaterMark;
    private int writeBufHighWaterMark;
    private int writeFlowControlLimit;
    private int readFlowControlLimit;
    private int maxResponseFrameLength;

    private int maxConnectionsOnEachServer;
    private boolean flushChannelOnEachWrite;

    private int channelMaxIdleTimeSeconds;

    private StickyConfig stickyConfig;

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

    public int getWriteFlowControlLimit() {
        return writeFlowControlLimit;
    }

    public int getReadFlowControlLimit() {
        return readFlowControlLimit;
    }

    public int getMaxResponseFrameLength() {
        return maxResponseFrameLength;
    }

    public int getMaxConnectionsOnEachServer() {
        return maxConnectionsOnEachServer;
    }

    public boolean isFlushChannelOnEachWrite() {
        return flushChannelOnEachWrite;
    }

    public int getChannelMaxIdleTimeSeconds() {
        return channelMaxIdleTimeSeconds;
    }

    public StickyConfig getStickyConfig() {
        return stickyConfig;
    }

    public static class Builder {
        private int connectTimeoutMillis = 3000;
        private int socketSndBufSize = 65535;
        private int socketRcvBufSize = 65535;
        private int writeBufLowWaterMark = 32 * 1024;
        private int writeBufHighWaterMark = 64 * 1024;
        private int writeFlowControlLimit;
        private int readFlowControlLimit;
        private int maxResponseFrameLength = 150 * 1024 * 1024;

        private int maxConnectionsOnEachServer = 6;
        private boolean flushChannelOnEachWrite = true;

        private int channelMaxIdleTimeSeconds = 600;

        private StickyConfig.Builder stickyConfigBuilder;

        public Builder connectTimeoutMillis(int connectTimeoutMillis) {
            this.connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        public Builder socketSndBufSize(int socketSndBufSize) {
            this.socketSndBufSize = socketSndBufSize;
            return this;
        }

        public Builder socketRcvBufSize(int socketRcvBufSize) {
            this.socketRcvBufSize = socketRcvBufSize;
            return this;
        }

        public Builder writeBufLowWaterMark(int writeBufLowWaterMark) {
            this.writeBufLowWaterMark = writeBufLowWaterMark;
            return this;
        }

        public Builder writeBufHighWaterMark(int writeBufHighWaterMark) {
            this.writeBufHighWaterMark = writeBufHighWaterMark;
            return this;
        }

        public Builder writeFlowControlLimit(int writeFlowControlLimit) {
            this.writeFlowControlLimit = writeFlowControlLimit;
            return this;
        }

        public Builder readFlowControlLimit(int readFlowControlLimit) {
            this.readFlowControlLimit = readFlowControlLimit;
            return this;
        }

        public Builder maxResponseFrameLength(int maxResponseFrameLength) {
            this.maxResponseFrameLength = maxResponseFrameLength;
            return this;
        }

        public Builder maxConnectionsOnEachServer(int maxConnectionsOnEachServer) {
            this.maxConnectionsOnEachServer = maxConnectionsOnEachServer;
            return this;
        }

        public Builder flushChannelOnEachWrite(boolean flushChannelOnEachWrite) {
            this.flushChannelOnEachWrite = flushChannelOnEachWrite;
            return this;
        }

        public Builder channelMaxIdleTimeSeconds(int channelMaxIdleTimeSeconds) {
            this.channelMaxIdleTimeSeconds = channelMaxIdleTimeSeconds;
            return this;
        }

        public Builder stickyWorkerNum(int workerNum) {
            if (stickyConfigBuilder == null) {
                stickyConfigBuilder = new StickyConfig.Builder();
            }
            this.stickyConfigBuilder.stickyWorkerNum(workerNum);
            return this;
        }

        public Builder stickyBatchSize(int batchSize) {
            if (stickyConfigBuilder == null) {
                stickyConfigBuilder = new StickyConfig.Builder();
            }
            this.stickyConfigBuilder.stickyBatchSize(batchSize);
            return this;
        }

        public ClientConfig build() {
            ClientConfig config = new ClientConfig();

            config.connectTimeoutMillis = this.connectTimeoutMillis;
            config.socketSndBufSize = this.socketSndBufSize;
            config.socketRcvBufSize = this.socketRcvBufSize;
            config.writeBufLowWaterMark = this.writeBufLowWaterMark;
            config.writeBufHighWaterMark = this.writeBufHighWaterMark;
            config.writeFlowControlLimit = this.writeFlowControlLimit;
            config.readFlowControlLimit = this.readFlowControlLimit;
            config.maxResponseFrameLength = this.maxResponseFrameLength;
            config.maxConnectionsOnEachServer = this.maxConnectionsOnEachServer;
            config.flushChannelOnEachWrite = this.flushChannelOnEachWrite;
            config.channelMaxIdleTimeSeconds = this.channelMaxIdleTimeSeconds;

            if (stickyConfigBuilder != null) {
                config.stickyConfig = stickyConfigBuilder.build();
            }
            return config;
        }
    }

    public static class StickyConfig {
        private int workerNum;
        private int batchSize;

        private StickyConfig(int workerNum, int batchSize) {
            this.workerNum = workerNum;
            this.batchSize = batchSize;
        }

        public int getWorkerNum() {
            return workerNum;
        }

        public int getBatchSize() {
            return batchSize;
        }

        private static class Builder {
            private int workerNum = Runtime.getRuntime().availableProcessors() / 2;
            private int batchSize = 512;

            private Builder stickyWorkerNum(int workerNum) {
                this.workerNum = workerNum;
                return this;
            }

            private Builder stickyBatchSize(int batchSize) {
                this.batchSize = batchSize;
                return this;
            }

            private StickyConfig build() {
                Assert.isPositive(workerNum);
                Assert.isPositive(batchSize);

                if (workerNum > Runtime.getRuntime().availableProcessors()) {
                    workerNum = Runtime.getRuntime().availableProcessors();
                }
                return new StickyConfig(workerNum, batchSize);
            }
        }
    }
}
