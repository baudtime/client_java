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

package io.baudtime.demo;

import io.baudtime.client.Client;
import io.baudtime.client.ClientBuilder;
import io.baudtime.client.WriteResponseHook;
import io.baudtime.discovery.StaticServiceAddrProvider;
import io.baudtime.message.GeneralResponse;
import io.baudtime.message.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class Demo {
    private static final Logger logger = LoggerFactory.getLogger(Demo.class);
    private static String[] addrs = {"127.0.0.1:8088", "localhost:8088"};

    public static void main(String[] args) {
        Client cli = new ClientBuilder().serviceAddrProvider(new StaticServiceAddrProvider(addrs)).writeResponseHook(new WriteResponseHook() {
            @Override
            public void onReceiveResponse(long opaque, GeneralResponse writeResponse) {
                logger.info("{} {}", opaque, writeResponse.getMessage());
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