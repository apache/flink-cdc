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

package org.apache.flink.cdc.debezium.table;

import java.util.Map;
import java.util.Properties;

/** Option utils for Debezium options. */
public class DebeziumOptions {
    public static final String DEBEZIUM_OPTIONS_PREFIX = "debezium.";

    public static Properties getDebeziumProperties(Map<String, String> properties) {
        final Properties debeziumProperties = new Properties();

        if (hasDebeziumProperties(properties)) {
            properties.keySet().stream()
                    .filter(key -> key.startsWith(DEBEZIUM_OPTIONS_PREFIX))
                    .forEach(
                            key -> {
                                final String value = properties.get(key);
                                final String subKey =
                                        key.substring((DEBEZIUM_OPTIONS_PREFIX).length());
                                debeziumProperties.put(subKey, value);
                            });
        }
        if (properties.containsKey("table.include.list")
                || properties.containsKey("table.exclude.list")) {
            throw new IllegalArgumentException(
                    "table.include.list and table.exclude.list are not supported to set manually, please remove these options.");
        }
        return debeziumProperties;
    }

    /**
     * Decides if the table options contains Debezium client properties that start with prefix
     * 'debezium'.
     */
    private static boolean hasDebeziumProperties(Map<String, String> debeziumOptions) {
        return debeziumOptions.keySet().stream()
                .anyMatch(k -> k.startsWith(DEBEZIUM_OPTIONS_PREFIX));
    }
}
