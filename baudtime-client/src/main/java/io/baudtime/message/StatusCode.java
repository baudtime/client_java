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

public enum StatusCode {
    Succeed((byte) 0), Failed((byte) 1);

    private final byte value;

    StatusCode(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static StatusCode parse(byte b) {
        if (b == StatusCode.Succeed.value) {
            return StatusCode.Succeed;
        }
        if (b == StatusCode.Failed.value) {
            return StatusCode.Failed;
        }
        throw new RuntimeException("bad type");
    }
}
