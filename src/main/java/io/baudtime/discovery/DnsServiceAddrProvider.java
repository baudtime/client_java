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

package io.baudtime.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.*;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DnsServiceAddrProvider extends StaticServiceAddrProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DnsServiceAddrProvider.class);
    private final Lookup lookup;
    private int servicePort;
    private String hostsFingerprint;

    public DnsServiceAddrProvider(String domainName, int servicePort) {
        this(domainName, servicePort, 60, TimeUnit.SECONDS);
    }

    public DnsServiceAddrProvider(String domainName, int servicePort, long checkInterval, TimeUnit timeUnit) {
        try {
            lookup = new Lookup(Name.fromString(domainName));
        } catch (TextParseException e) {
            throw new RuntimeException(e);
        }

        this.servicePort = servicePort;

        ArrayList<String> addrs = new ArrayList<String>();
        if ("localhost".equalsIgnoreCase(domainName) || "127.0.0.1".equals(domainName)) {
            addrs.add("127.0.0.1:" + servicePort);
        } else {
            Record[] records = lookup.run();
            if (records != null) {
                for (Record record : records) {
                    ARecord a = (ARecord) record;
                    InetAddress address = a.getAddress();
                    if (address != null) {
                        addrs.add(address.getHostAddress() + ":" + servicePort);
                    }
                }
                Collections.shuffle(addrs);
            }
        }

        this.healthyAddrs = addrs;
        this.checkInterval = checkInterval;
        this.checkTimeUnit = timeUnit;
    }

    @Override
    public void watch() {
        super.watch();

        watcher.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    Lookup.getDefaultCache(DClass.IN).clearCache();
                    Record[] records = lookup.run();

                    if (records != null) {
                        TreeSet<String> hosts = new TreeSet<String>();

                        for (Record record : records) {
                            ARecord a = (ARecord) record;
                            InetAddress address = a.getAddress();
                            if (address != null) {
                                hosts.add(address.getHostAddress());
                            }
                        }

                        ArrayList<String> dummyAddrs = new ArrayList<String>();
                        String newHostsFingerprint = computeFingerprint(hosts, dummyAddrs);
                        if (hostsFingerprint == null || !hostsFingerprint.equals(newHostsFingerprint)) {
                            Collections.shuffle(dummyAddrs);

                            ReentrantReadWriteLock.WriteLock l = DnsServiceAddrProvider.this.addrsLock.writeLock();

                            l.lock();
                            DnsServiceAddrProvider.this.healthyAddrs = dummyAddrs;
                            hostsFingerprint = newHostsFingerprint;
                            l.unlock();

                            for (ServiceAddrObserver o : observers) {
                                o.addrChanged();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("watch DNS change error", e);
                }
            }
        }, 0, checkInterval, checkTimeUnit);
    }

    private String computeFingerprint(TreeSet<String> addrs, ArrayList<String> dummyAddrs) {
        Iterator<String> it = addrs.iterator();
        if (!it.hasNext())
            return "";

        StringBuilder sb = new StringBuilder();
        for (; ; ) {
            String e = it.next();
            if (!"".equals(e)) {
                dummyAddrs.add(e + ":" + this.servicePort);
                sb.append(e);
            }
            if (!it.hasNext())
                return sb.toString();
            sb.append(' ');
        }
    }
}
