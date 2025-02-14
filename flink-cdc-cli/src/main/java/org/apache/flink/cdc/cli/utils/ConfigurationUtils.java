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

package org.apache.flink.cdc.cli.utils;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utilities for handling {@link Configuration}. */
public class ConfigurationUtils {

    private static final String KEY_SEPARATOR = ".";

    public static Configuration loadConfigFile(Path configPath) throws Exception {
        return loadConfigFile(configPath, false);
    }

    public static Configuration loadConfigFile(Path configPath, boolean allowDuplicateKeys)
            throws Exception {
        Map<String, Object> configMap =
                YamlParserUtils.loadYamlFile(
                        new File(configPath.toUri().getPath()), allowDuplicateKeys);
        return Configuration.fromMap(flattenConfigMap(configMap, ""));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> flattenConfigMap(
            Map<String, Object> config, String keyPrefix) {
        final Map<String, String> flattenedMap = new HashMap<>();

        config.forEach(
                (key, value) -> {
                    String flattenedKey = keyPrefix + key;
                    if (value instanceof Map) {
                        Map<String, Object> e = (Map<String, Object>) value;
                        flattenedMap.putAll(flattenConfigMap(e, flattenedKey + KEY_SEPARATOR));
                    } else {
                        if (value instanceof List) {
                            flattenedMap.put(flattenedKey, YamlParserUtils.toYAMLString(value));
                        } else {
                            flattenedMap.put(flattenedKey, value.toString());
                        }
                    }
                });

        return flattenedMap;
    }

    public static Class<?> getClaimModeClass() {
        try {
            return Class.forName("org.apache.flink.core.execution.RestoreMode");
        } catch (ClassNotFoundException ignored) {
            try {
                return Class.forName("org.apache.flink.runtime.jobgraph.RestoreMode");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
