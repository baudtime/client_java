package io.baudtime.demo;

import io.baudtime.client.ClientBuilder;
import io.baudtime.client.MultiEndpointClient;
import io.baudtime.client.RecordsAdaptor;
import io.baudtime.client.netty.Future;
import io.baudtime.client.netty.FutureListener;
import io.baudtime.discovery.StaticServiceAddrProvider;
import io.baudtime.message.Label;
import io.baudtime.message.Point;
import io.baudtime.message.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class Batch {
    private static final Logger logger = LoggerFactory.getLogger(Demo.class);
    private static String[] addrs = {"127.0.0.1:8088"};

    public static void main(String[] args) {
        MultiEndpointClient cli = ClientBuilder.newMultiEndpointClientBuilder().flushChannelOnEachWrite(true)
                .serviceAddrProvider("default", new StaticServiceAddrProvider(2, TimeUnit.SECONDS, addrs))
                .writeResponseHook(new FutureListener() {
                    @Override
                    public void onFinished(Future future) {
                        logger.info("{}", future.getOpaque());
                    }
                }).stickyWorkerNum(2).stickyBatchSize(512).stickyQueueCapacity(1024).build();

        RecordsAdaptor<MultiEndpointClient, HashMap<Long, Series>> wrapped = RecordsAdaptor.wrap(cli, new RecordsAdaptor.RecordsConverter<HashMap<Long, Series>>() {
            @Override
            public Collection<Series> convert(HashMap<Long, Series> series) {
                return series.values();
            }
        });

        wrapped.raw().use("default");

        try {
            long lastT = 0;

            Series.Builder sb1 = Series.newBuilder();

            sb1.setMetricName("cnt");
            sb1.addLabel("host", "localhost");
            sb1.addLabel("app", "testapp");
            sb1.addLabel("idc", "langfang");
            Label.Builder lb = sb1.addLabelBuilder();
            Point.Builder pb = sb1.addPointBuilder();

            while (true) {
                HashMap<Long, Series> m = new HashMap<Long, Series>();

                long t = System.currentTimeMillis();
                while (t <= lastT) {
                    t = System.currentTimeMillis();
                }
                lastT = t;

                for (int i = 0; i < 10; i++) {
                    lb.setName("state").setValue(String.valueOf(i));
                    pb.setT(t - i).setV(i);

                    m.put(t, sb1.build());
                }

                try {
                    wrapped.write(m);
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        } finally {
            cli.close();
        }
    }
}