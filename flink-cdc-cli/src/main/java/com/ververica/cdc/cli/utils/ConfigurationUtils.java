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

package com.ververica.cdc.cli.utils;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Utilities for handling {@link Configuration} in Flink. */
public class ConfigurationUtils {
    public static Configuration loadMapFormattedConfig(Path configPath) throws Exception {
        if (!Files.exists(configPath)) {
            throw new FileNotFoundException(
                    String.format("Cannot find configuration file at \"%s\"", configPath));
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            Map<String, String> configMap =
                    mapper.readValue(
                            configPath.toFile(), new TypeReference<Map<String, String>>() {});
            return Configuration.fromMap(configMap);
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to load config file \"%s\" to key-value pairs", configPath),
                    e);
        }
    }
}
