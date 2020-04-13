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

package io.baudtime.message;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessageCheck {

    private static final Pattern labelNameRE = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    private static final String labelNameREMsg = "label name must match ^[a-zA-Z_][a-zA-Z0-9_]*$";

    public static class BuildException extends RuntimeException {
        public BuildException(String msg) {
            super(msg);
        }

        public BuildException() {
        }
    }

    public static class MarshalException extends RuntimeException {
        public MarshalException(String msg) {
            super(msg);
        }

        public MarshalException(Throwable t) {
            super(t);
        }
    }

    public static class UnmarshalException extends RuntimeException {
        public UnmarshalException(String msg) {
            super(msg);
        }

        public UnmarshalException(Throwable t) {
            super(t);
        }
    }

    public static void checkLabelName(String name) {
        Matcher m = labelNameRE.matcher(name);
        if (!m.matches()) {
            throw new MessageCheck.BuildException(labelNameREMsg);
        }
    }
}
