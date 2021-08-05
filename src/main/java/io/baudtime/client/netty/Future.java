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
import io.baudtime.message.Recyclable;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Future implements ChannelFutureListener {
    private final Long opaque;
    private volatile BaudMessage request;
    private volatile BaudMessage response;

    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private List<FutureListener> listeners;

    public Future(long opaque) {
        this.opaque = opaque;
    }

    public Future(Message msg) {
        this.opaque = msg.getOpaque();
        this.request = msg.getRaw();
    }

    public Future addListener(FutureListener listener) {
        if (listeners == null) {
            listeners = new LinkedList<FutureListener>();
        }
        listeners.add(listener);
        return this;
    }

    public Long getOpaque() {
        return opaque;
    }

    public BaudMessage await(long timeout, TimeUnit unit) throws InterruptedException {
        if (!this.countDownLatch.await(timeout, unit)) {
            throw new RuntimeException("response timed out");
        }
        return this.response;
    }

    public BaudMessage getResponse() {
        return response;
    }

    public Future setResponse(BaudMessage response) {
        this.response = response;
        return this;
    }

    public Throwable getCause() {
        return cause;
    }

    public Future setCause(Throwable cause) {
        this.cause = cause;
        return this;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public Future setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
        return this;
    }

    public Future finish() {
        this.countDownLatch.countDown();
        if (listeners != null) {
            for (FutureListener listener : listeners) {
                listener.onFinished(this);
            }
        }
        return this;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        try {
            future.removeListener(this);
            if (future.isSuccess()) {
                this.setSendRequestOK(true);
            } else {
                this.setSendRequestOK(false).setCause(future.cause()).finish();
            }
        } finally {
            if (this.request != null) {
                if (request instanceof Recyclable) {
                    ((Recyclable) request).recycle();
                }
                this.request = null;
            }
        }
    }
}
