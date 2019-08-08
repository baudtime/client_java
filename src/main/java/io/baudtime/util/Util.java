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

package io.baudtime.util;

import io.netty.buffer.ByteBuf;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Util {
    public static byte[] intToFixedLengthBytes(int a) {
        byte[] ret = new byte[4];
        ret[0] = (byte) ((a >> 24) & 0xFF);
        ret[1] = (byte) ((a >> 16) & 0xFF);
        ret[2] = (byte) ((a >> 8) & 0xFF);
        ret[3] = (byte) (a & 0xFF);
        return ret;
    }

    public static byte[] varLongToBytes(long value) {
        byte[] byteArrayList = new byte[10];
        int i = 0;
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            byteArrayList[i++] = ((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        byteArrayList[i] = ((byte) (value & 0x7F));
        byte[] out = new byte[i + 1];
        for (; i >= 0; i--) {
            out[i] = byteArrayList[i];
        }
        return out;
    }

    public static long readVarLong(ByteBuf in) {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.readByte()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    public static long exponential(long d, long min, long max) {
        d *= 2;
        if (d < min) {
            d = min;
        }
        if (d > max) {
            d = max;
        }
        return d;
    }

    public static class OS {
        private static String osName = System.getProperty("os.name").toLowerCase();

        public static boolean isLinux() {
            return osName.indexOf("linux") >= 0;
        }

        public static boolean isUnix() {
            return osName.indexOf("nix") >= 0 || (osName.indexOf("mac") >= 0 && osName.indexOf("os") > 0) || osName.indexOf("bsd") > 0;
        }
    }

    public static class Formatter {
        private static ThreadLocal<DateFormat> fmt = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'");
                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                return df;
            }
        };

        public static String format(Date date) {
            return fmt.get().format(date);
        }
    }
}
