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

package org.apache.flink.cdc.common.shade.utils;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.shade.ConfigShade;
import org.apache.flink.cdc.common.utils.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BiFunction;

/** Config shade utilities. */
public final class ConfigShadeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigShadeUtils.class);

    private static final Map<String, ConfigShade> CONFIG_SHADES = new HashMap<>();

    private static final ConfigShade DEFAULT_SHADE = new DefaultConfigShade();
    private static final String SOURCE_KEY = "source";
    private static final String SINK_KEY = "sink";

    static {
        ServiceLoader<ConfigShade> serviceLoader = ServiceLoader.load(ConfigShade.class);
        Iterator<ConfigShade> it = serviceLoader.iterator();
        it.forEachRemaining(
                configShade -> {
                    CONFIG_SHADES.put(configShade.getIdentifier(), configShade);
                });
        LOG.info("Load config shade spi: {}", CONFIG_SHADES.keySet());
    }

    @VisibleForTesting
    public static String decryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return configShade.decrypt(content);
    }

    public static JsonNode decryptConfig(JsonNode root, Configuration pipelineConfig)
            throws Exception {
        String identifier = pipelineConfig.get(PipelineOptions.SHADE_IDENTIFIER_OPTION);
        List<String> sensitiveOptions =
                pipelineConfig.get(PipelineOptions.SHADE_SENSITIVE_KEYWORDS);
        return decryptConfig(identifier, root, sensitiveOptions, pipelineConfig);
    }

    @SuppressWarnings("unchecked")
    public static JsonNode decryptConfig(
            String identifier,
            JsonNode root,
            List<String> sensitiveOptions,
            Configuration pipelineConfig)
            throws Exception {
        ConfigShade configShade = CONFIG_SHADES.get(identifier);
        if (configShade == null) {
            LOG.error("Can not find config shade: {}", identifier);
            throw new IllegalStateException("Can not find config shade: " + identifier);
        }
        configShade.initialize(pipelineConfig);

        if (DEFAULT_SHADE.getIdentifier().equals(configShade.getIdentifier())) {
            return root;
        }

        LOG.info("Use config shade: {}", identifier);
        BiFunction<String, Object, String> processFunction =
                (key, value) -> configShade.decrypt(value.toString());
        ObjectNode jsonNodes = (ObjectNode) root;
        Map<String, Object> configMap = JsonUtils.toMap(jsonNodes);
        Map<String, Object> source = (Map<String, Object>) configMap.get(SOURCE_KEY);
        Map<String, Object> sink = (Map<String, Object>) configMap.get(SINK_KEY);
        Preconditions.checkArgument(
                !source.isEmpty(), "Miss <Source> config! Please check the config file.");
        Preconditions.checkArgument(
                !sink.isEmpty(), "Miss <Sink> config! Please check the config file.");

        for (String sensitiveOption : sensitiveOptions) {
            source.computeIfPresent(sensitiveOption, processFunction);
            sink.computeIfPresent(sensitiveOption, processFunction);
        }
        LOG.info("{} for source/sink has been decrypted and refreshed", sensitiveOptions);

        configMap.put(SOURCE_KEY, source);
        configMap.put(SINK_KEY, sink);
        return JsonUtils.toJsonNode(configMap);
    }

    /** Default ConfigShade. */
    public static class DefaultConfigShade implements ConfigShade {
        private static final String IDENTIFIER = "default";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String decrypt(String content) {
            return content;
        }
    }
}
