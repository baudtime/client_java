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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ResponseFuture {
    private final Long opaque;

    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private volatile BaudMessage response;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    public ResponseFuture(long opaque) {
        this.opaque = opaque;
    }

    public BaudMessage await(long timeout, TimeUnit unit) throws InterruptedException {
        if (!this.countDownLatch.await(timeout, unit)) {
            throw new RuntimeException("response timed out");
        }
        return this.response;
    }

    public void putResponse(final BaudMessage response) {
        this.response = response;
        this.countDownLatch.countDown();
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public BaudMessage getResponse() {
        return response;
    }

    public void setResponse(BaudMessage response) {
        this.response = response;
    }

    public Long getOpaque() {
        return opaque;
    }

    @Override
    public String toString() {
        return "ResponseFuture [response=" + response + ", sendRequestOK=" + sendRequestOK
                + ", cause=" + cause + ", opaque=" + opaque
                + ", timeoutMillis=" + ", countDownLatch=" + countDownLatch + "]";
    }
}
