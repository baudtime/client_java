package io.baudtime.demo;

import io.baudtime.client.Client;
import io.baudtime.client.ClientBuilder;
import io.baudtime.client.netty.Future;
import io.baudtime.client.netty.FutureListener;
import io.baudtime.discovery.StaticServiceAddrProvider;
import io.baudtime.message.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class Demo {
    private static final Logger logger = LoggerFactory.getLogger(Demo.class);
    private static String[] addrs = {"127.0.0.1:8088", "localhost:8088"};

    public static void main(String[] args) {
        Client cli = ClientBuilder.newClientBuilder().serviceAddrProvider(new StaticServiceAddrProvider(addrs)).writeResponseHook(new FutureListener() {
            @Override
            public void onFinished(Future future) {
                logger.info("{}", future.getOpaque());
            }
        }).build();

        try {
            QueryResponse resp = cli.instantQuery("cnt[10m]", null, 3, TimeUnit.SECONDS);
            logger.info("{} {} {}", resp.getResult(), resp.getStatus(), resp.getErrorMsg());
        } finally {
            if (cli != null) {
                cli.close();
            }
        }
    }
}