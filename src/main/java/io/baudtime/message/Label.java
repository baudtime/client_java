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

import io.baudtime.util.Assert;

import java.util.Comparator;

import static io.baudtime.util.LabelChecker.checkLabelName;

public class Label {
    private final String name;
    private final String value;

    public Label(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private String value;

        private boolean noLabelNameCheck = false;

        public Builder noLabelNameCheck() {
            this.noLabelNameCheck = true;
            return this;
        }

        public Builder setName(String name) {
            if (!noLabelNameCheck) {
                checkLabelName(name);
            }
            this.name = name;
            return this;
        }

        public Builder setValue(String value) {
            this.value = value;
            return this;
        }

        public Label build() {
            Assert.notEmpty(this.name);
            Assert.notEmpty(this.value);

            return new Label(this.name, this.value);
        }
    }

    public static final Comparator<Label> comparator = new Comparator<Label>() {
        @Override
        public int compare(Label l1, Label l2) {
            int d = l1.getName().compareTo(l2.getName());
            if (d != 0) {
                return d;
            }

            return l1.getValue().compareTo(l2.getValue());
        }
    };
}
