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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StaticServiceAddrProvider implements ServiceAddrProvider {
    private static final Logger log = LoggerFactory.getLogger(StaticServiceAddrProvider.class);

    protected List<String> healthyAddrs;
    protected Set<String> unhealthyAddrs = new HashSet<String>(128);
    protected final ReentrantReadWriteLock addrsLock = new ReentrantReadWriteLock();

    private final AtomicInteger iter = new AtomicInteger();

    protected final ScheduledExecutorService watcher = Executors.newSingleThreadScheduledExecutor();
    protected long checkInterval;
    protected TimeUnit checkTimeUnit;

    //an addr is like "192.168.0.1:8087"
    public StaticServiceAddrProvider(String... addrs) {
        this(15, TimeUnit.SECONDS, addrs);
    }

    public StaticServiceAddrProvider(List<String> addrs) {
        this(15, TimeUnit.SECONDS, addrs);
    }

    public StaticServiceAddrProvider(long checkInterval, TimeUnit timeUnit, String... addrs) {
        this.checkInterval = checkInterval;
        this.checkTimeUnit = timeUnit;

        this.healthyAddrs = new LinkedList<String>(Arrays.asList(addrs));
        Collections.shuffle(this.healthyAddrs);
    }

    public StaticServiceAddrProvider(long checkInterval, TimeUnit timeUnit, List<String> addrs) {
        this.checkInterval = checkInterval;
        this.checkTimeUnit = timeUnit;

        this.healthyAddrs = addrs;
        Collections.shuffle(this.healthyAddrs);
    }

    @Override
    public String getServiceAddr() {
        int healthyHostNum = 0;

        ReentrantReadWriteLock.ReadLock l = addrsLock.readLock();
        l.lock();
        try {
            healthyHostNum = healthyAddrs.size();
            if (healthyHostNum <= 0) {
                return null;
            }

            int i, j;
            do {
                i = iter.get();
                j = (i + 1) % healthyHostNum;
            } while (!iter.compareAndSet(i, j));

            return healthyAddrs.get(j);
        } finally {
            l.unlock();
            if (healthyHostNum <= 0) {
                checkUnhealthyAddrs();
            }
        }
    }

    @Override
    public void serviceDown(String addr) {
        if (ping(addr)) {
            return;
        }

        log.info("service {} down", addr);
        ReentrantReadWriteLock.WriteLock l = addrsLock.writeLock();

        l.lock();
        try {
            healthyAddrs.remove(addr);
            unhealthyAddrs.add(addr);
        } finally {
            l.unlock();
        }
    }

    @Override
    public void serviceRecover(String addr) {
        log.info("service {} recover", addr);
        ReentrantReadWriteLock.WriteLock l = addrsLock.writeLock();

        l.lock();
        try {
            if (!healthyAddrs.contains(addr)) {
                healthyAddrs.add(addr);
            }
            unhealthyAddrs.remove(addr);
            Collections.shuffle(healthyAddrs);
        } finally {
            l.unlock();
        }
    }

    @Override
    public void watch() {
        watcher.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                StaticServiceAddrProvider.this.checkUnhealthyAddrs();
            }
        }, 0, checkInterval, checkTimeUnit);
    }

    @Override
    public void stopWatch() {
        watcher.shutdownNow();
    }

    private void checkUnhealthyAddrs() {
        Set<String> toRecover = new HashSet<String>();

        ReentrantReadWriteLock.ReadLock l = addrsLock.readLock();
        l.lock();

        try {
            log.debug("all unhealthy addrs", unhealthyAddrs.toArray());
            for (String addr : unhealthyAddrs) {
                if (ping(addr)) {
                    toRecover.add(addr);
                }
            }
        } finally {
            l.unlock();
        }

        for (String addr : toRecover) {
            serviceRecover(addr);
        }
    }

    private boolean ping(String addr) {
        String[] s = addr.split(":");
        if (s.length != 2) {
            throw new RuntimeException("invalid format of addr");
        }

        SocketAddress address = new InetSocketAddress(s[0], Integer.parseInt(s[1]));
        Socket socket = new Socket();

        try {
            try {
                socket.connect(address, 300);
                return socket.isConnected();
            } finally {
                socket.close();
            }
        } catch (IOException e) {

        }
        return false;
    }
}
