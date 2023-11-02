/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source.utils;

import java.util.Map;
import java.util.Properties;

/** Option utils for listener properties. */
public class ListenerUtils {

    // Prefix for listener properties.
    public static final String PROPERTIES_PREFIX = "external-system.";

    public static Properties getListenerProperties(Map<String, String> tableOptions) {
        Properties listenerProperties = new Properties();
        if (tableOptions != null) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                listenerProperties.put(subKey, value);
                            });
        }
        return listenerProperties;
    }
}
