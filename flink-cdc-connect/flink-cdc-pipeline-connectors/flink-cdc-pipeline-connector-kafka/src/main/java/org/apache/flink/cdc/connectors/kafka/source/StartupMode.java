/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kafka.source;

/** Startup mode for Kafka consumer. */
public enum StartupMode {
    EARLIEST_OFFSET("earliest-offset"),

    LATEST_OFFSET("latest-offset"),

    GROUP_OFFSETS("group-offsets"),

    TIMESTAMP("timestamp"),

    SPECIFIC_OFFSETS("specific-offsets");

    private final String value;

    StartupMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static StartupMode fromValue(String value) {
        for (StartupMode mode : values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported startup mode: "
                        + value
                        + ". "
                        + "Supported modes are: earliest-offset, latest-offset, "
                        + "group-offsets, timestamp, specific-offsets");
    }
}
