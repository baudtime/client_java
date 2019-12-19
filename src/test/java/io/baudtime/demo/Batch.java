package io.baudtime.demo;

import io.baudtime.client.Client;
import io.baudtime.client.ClientBuilder;
import io.baudtime.client.WriteResponseHook;
import io.baudtime.discovery.StaticServiceAddrProvider;
import io.baudtime.message.GeneralResponse;
import io.baudtime.message.Label;
import io.baudtime.message.Point;
import io.baudtime.message.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Batch {
    private static final Logger logger = LoggerFactory.getLogger(Demo.class);
    private static String[] addrs = {"127.0.0.1:8088"};

    public static void main(String[] args) {
        Client cli = new ClientBuilder().flushChannelOnEachWrite(true)
                .serviceAddrProvider(new StaticServiceAddrProvider(2, TimeUnit.SECONDS, addrs))
                .writeResponseHook(new WriteResponseHook() {
                    @Override
                    public void onReceiveResponse(long opaque, GeneralResponse response) {
                        logger.info("{} {} {}", opaque, response.getMessage(), response.getStatus());
                    }
                }).build();

        try {
            List<Series> s = new LinkedList<Series>();
            long lastT = 0;

            Series.Builder sb1 = Series.newBuilder();

            sb1.setMetricName("cnt");
            sb1.addLabel("host", "localhost");
            sb1.addLabel("app", "testapp");
            sb1.addLabel("idc", "langfang");
            Label.Builder lb = sb1.addLabelBuilder();
            Point.Builder pb = sb1.addPointBuilder();

            while (true) {
                long t = System.currentTimeMillis();
                while (t <= lastT) {
                    t++;
                    t = System.currentTimeMillis();
                }
                lastT = t;

                for (int i = 0; i < 10; i++) {
                    lb.setName("state").setValue(String.valueOf(i));
                    pb.setT(t - i).setV(i);

                    s.add(sb1.build());
                }

                try {
                    cli.write(s);
                    s.clear();
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        } finally {
            cli.close();
        }
    }
}