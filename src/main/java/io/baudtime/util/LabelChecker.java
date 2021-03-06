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

import io.baudtime.message.Exceptions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LabelChecker {
    private static final Pattern labelNameRE = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    private static final ThreadLocal<Matcher> matchers = new ThreadLocal<Matcher>();

    public static void checkMetricName(String name) {
        checkLabelName(name);
    }

    public static void checkLabelName(String name) {
        Matcher m = matchers.get();
        if (m == null) {
            m = labelNameRE.matcher(name);
            matchers.set(m);
        } else {
            m.reset(name);
        }

        if (!m.matches()) {
            throw new Exceptions.LabelPatternException(name);
        }
    }
}
