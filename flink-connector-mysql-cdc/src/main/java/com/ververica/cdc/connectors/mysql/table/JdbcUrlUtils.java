/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import java.util.Map;
import java.util.Properties;

/** Option utils for JDBC URL properties. */
public class JdbcUrlUtils {

    // Prefix for JDBC specific properties.
    public static final String PROPERTIES_PREFIX = "jdbc.properties.";

    public static Properties getJdbcProperties(Map<String, String> tableOptions) {
        Properties jdbcProperties = new Properties();
        if (hasJdbcProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                jdbcProperties.put(subKey, value);
                            });
        }
        return jdbcProperties;
    }

    /**
     * Decides if the table options contains JDBC properties that start with prefix
     * 'jdbc.properties'.
     */
    private static boolean hasJdbcProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }
}
