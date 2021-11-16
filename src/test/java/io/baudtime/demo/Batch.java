package io.baudtime.demo;

import io.baudtime.client.Client;
import io.baudtime.client.ClientBuilder;
import io.baudtime.client.netty.Future;
import io.baudtime.client.netty.FutureListener;
import io.baudtime.discovery.StaticServiceAddrProvider;
import io.baudtime.message.Label;
import io.baudtime.message.Point;
import io.baudtime.message.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Batch {
    private static final Logger logger = LoggerFactory.getLogger(Demo.class);
    private static String[] addrs = {"127.0.0.1:7630"};

    public static void main(String[] args) {
        Client cli = ClientBuilder.newClientBuilder().flushChannelOnEachWrite(true)
                .serviceAddrProvider(new StaticServiceAddrProvider(2, TimeUnit.SECONDS, addrs))
                .writeResponseHook(new FutureListener() {
                    @Override
                    public void onFinished(Future future) {
                        logger.info("{} {}", future.getOpaque(), future.getCause());
                    }
                }).build();

        try {
            Series.Builder sb1 = Series.newBuilder();

            sb1.setMetricName("cnt");
            sb1.addLabel("host", "localhost");
            sb1.addLabel("app", "testapp");
            sb1.addLabel("idc", "langfang");
            Label.Builder lb = sb1.addLabelBuilder();
            Point.Builder pb = sb1.addPointBuilder();

            long t = System.currentTimeMillis();

            while (true) {
                List<Series> m = new ArrayList<Series>();
                t += 1000;

                for (int i = 0; i < 10; i++) {
                    lb.setName("state").setValue(String.valueOf(i));
                    pb.setT(t).setV(t);

                    m.add(sb1.build());
                }

                try {
                    cli.write(m);
                    Thread.sleep(1000);
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        } finally {
            cli.close();
        }
    }
}