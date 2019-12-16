package io.baudtime.client;

import io.baudtime.discovery.ServiceAddrProvider;
import io.baudtime.message.LabelValuesResponse;
import io.baudtime.message.QueryResponse;
import io.baudtime.message.Series;
import io.baudtime.message.SeriesLabelsResponse;
import io.netty.channel.ChannelFuture;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class MultiBaudClient implements Client {

    public static final RuntimeException haveNotSelectEndpoint = new RuntimeException("should select a endpoint first");
    public Client current;
    public ReentrantLock lock = new ReentrantLock();

    public ConcurrentHashMap<String, Client> clients = new ConcurrentHashMap<String, Client>();
    public final Map<String, ServiceAddrProvider> addrProviders;

    public ClientConfig config;
    public WriteResponseHook writeResponseHook;

    public MultiBaudClient(ClientConfig config, Map<String, ServiceAddrProvider> addrProviders, WriteResponseHook writeResponseHook) {
        this.config = config;
        this.addrProviders = addrProviders;
        this.writeResponseHook = writeResponseHook;
    }

    public Client use(String endpoint) {
        lock.lock();
        Client c = clients.get(endpoint);
        if (c != null) {
            current = c;
        } else {
            ServiceAddrProvider addrProvider = addrProviders.get(endpoint);
            if (addrProvider != null) {
                current = new BaudClient(this.config, addrProvider, writeResponseHook);
                clients.put(endpoint, current);
            } else {
                current = null;
            }
        }
        lock.unlock();

        return current;
    }

    @Override
    public QueryResponse instantQuery(String queryExp, Date time, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.instantQuery(queryExp, time, timeout, unit);
    }

    @Override
    public QueryResponse rangeQuery(String queryExp, Date start, Date end, long step, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.rangeQuery(queryExp, start, end, step, timeout, unit);
    }

    @Override
    public SeriesLabelsResponse seriesLabels(Collection<String> matches, Date start, Date end, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.seriesLabels(matches, start, end, timeout, unit);
    }

    @Override
    public LabelValuesResponse labelValues(String name, String constraint, long timeout, TimeUnit unit) {
        checkCurrentSelect();
        return current.labelValues(name, constraint, timeout, unit);
    }

    @Override
    public ChannelFuture write(Series... series) {
        checkCurrentSelect();
        return current.write(series);
    }

    @Override
    public ChannelFuture write(Collection<Series> series) {
        checkCurrentSelect();
        return current.write(series);
    }

    @Override
    public void close() {
        for (Client c : clients.values()) {
            c.close();
        }
    }

    private void checkCurrentSelect() {
        if (current == null) {
            throw haveNotSelectEndpoint;
        }
    }
}
