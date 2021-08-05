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
import io.baudtime.discovery.ServiceAddrObserver;
import io.baudtime.discovery.ServiceAddrProvider;
import io.baudtime.message.AddRequest;
import io.baudtime.message.Series;
import io.baudtime.util.Assert;
import io.netty.channel.Channel;

import java.util.Collection;
import java.util.concurrent.*;

public class KeyBoundClient<K> extends AbstractClient implements ServiceAddrObserver {

    public interface KeyMapping<K> {
        K getKey(Collection<Series> series);
    }

    private final KeyMapping<K> keyMapping;
    private final ConcurrentMap<K /* key, may be hash code */, Channel> channels = new ConcurrentHashMap<K, Channel>();

    private final ExecutorService updateChannelThread = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.DiscardPolicy());

    public KeyBoundClient(KeyMapping<K> keyMapping, ClientConfig clientConfig, ServiceAddrProvider serviceAddrProvider, FutureListener writeResponseHook) {
        super(clientConfig, serviceAddrProvider, writeResponseHook);
        Assert.notNull(keyMapping);
        this.keyMapping = keyMapping;

        serviceAddrProvider.addObserver(this);
    }

    @Override
    public void append(Collection<Series> series) {
        AddRequest.Builder reqBuilder = AddRequest.newBuilder();
        reqBuilder.addSeries(series);

        Channel ch = getChannelBySeries(series);
        asyncRequest(ch, reqBuilder.build());
    }

    private Channel getChannelBySeries(Collection<Series> series) {
        K key = keyMapping.getKey(series);

        Channel c = this.channels.get(key);
        if (c != null && c.isActive()) {
            return c;
        }

        synchronized (this.channels) {
            c = this.channels.get(key);
            if (c != null && c.isActive()) {
                return c;
            }

            Channel newc = getChannel();
            Channel oldc = this.channels.put(key, newc);
            if (oldc != null) {
                oldc.close();
                putChannel(oldc);
            }
            return newc;
        }
    }

    @Override
    public void addrChanged() {
        updateChannels();
    }

    @Override
    public void addrDown(String addr) {

    }

    @Override
    public void addrRecover(String addr) {
        updateChannels();
    }

    private void updateChannels() {
        updateChannelThread.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    //
                }

                synchronized (channels) {
                    for (Channel ch : channels.values()) {
                        if (ch != null) {
                            ch.close();
                            putChannel(ch);
                        }
                    }
                    channels.clear();
                }
            }
        });
    }
}
