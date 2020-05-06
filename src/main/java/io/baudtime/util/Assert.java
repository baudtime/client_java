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

public class Assert {

    public static class AssertException extends RuntimeException {
        private static final long serialVersionUID = 2963218315971075018L;

        public AssertException() {
            super();
        }

        public AssertException(String s) {
            super(s);
        }
    }

    public static void notNull(Object o) {
        notNull(o, null);
    }

    public static void notNull(Object o, String message) {
        if (o == null) {
            throw new AssertException(message);
        }
    }

    public static <T extends CharSequence> void notEmpty(T chars) {
        notEmpty(chars, null);
    }

    public static <T extends CharSequence> void notEmpty(T chars, String message) {
        if (chars == null || chars.length() == 0) {
            throw new AssertException(message);
        }
    }

    public static void equal(double a, double b) {
        equal(a, b, null);
    }

    public static void equal(double a, double b, String message) {
        if (a != b) {
            throw new AssertException(message);
        }
    }

    public static void notBiggerThan(double a, double b) {
        notBiggerThan(a, b, null);
    }

    public static void notBiggerThan(double a, double b, String message) {
        if (a > b) {
            throw new AssertException(message);
        }
    }

    public static void isNotNegative(double n) {
        isNotNegative(n, null);
    }

    public static void isNotNegative(double n, String message) {
        if (n < 0) {
            throw new AssertException(message);
        }
    }

    public static void isPositive(double n) {
        isPositive(n, null);
    }

    public static void isPositive(double n, String message) {
        if (n <= 0) {
            throw new AssertException(message);
        }
    }

}
