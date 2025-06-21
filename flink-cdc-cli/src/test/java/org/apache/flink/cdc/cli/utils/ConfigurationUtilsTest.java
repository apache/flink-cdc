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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.curator5.com.google.common.io.Resources;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/** Unit test for {@link org.apache.flink.cdc.cli.utils.ConfigurationUtils}. */
class ConfigurationUtilsTest {

    private static final Map<ConfigOption<?>, Object> CONFIG_OPTIONS = new HashMap<>();

    static {
        CONFIG_OPTIONS.put(
                ConfigOptions.key("jobmanager.rpc.address").stringType().noDefaultValue(),
                "localhost");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("jobmanager.rpc.port").stringType().noDefaultValue(), "6123");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("jobmanager.bind-host").stringType().noDefaultValue(),
                "localhost");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("jobmanager.memory.process.size").stringType().noDefaultValue(),
                "1600m");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("jobmanager.execution.failover-strategy")
                        .stringType()
                        .noDefaultValue(),
                "region");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("taskmanager.bind-host").stringType().noDefaultValue(),
                "localhost");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("taskmanager.host").stringType().noDefaultValue(), "localhost");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("taskmanager.memory.process.size").stringType().noDefaultValue(),
                "1728m");
        CONFIG_OPTIONS.put(
                ConfigOptions.key("taskmanager.numberOfTaskSlots").stringType().noDefaultValue(),
                "1");
    }

    @ParameterizedTest
    @ValueSource(strings = {"flink-home/conf/config.yaml", "flink-home/conf/flink-conf.yaml"})
    void loadConfigFile(String resourcePath) throws Exception {
        URL resource = Resources.getResource(resourcePath);
        Path path = new Path(resource.toURI());
        Configuration configuration =
                ConfigurationUtils.loadConfigFile(path, resourcePath.endsWith("flink-conf.yaml"));
        Map<String, String> configMap = configuration.toMap();
        for (Map.Entry<ConfigOption<?>, Object> entry : CONFIG_OPTIONS.entrySet()) {
            String key = entry.getKey().key();
            Object expectedValue = entry.getValue();
            Assertions.assertThat(configMap)
                    .containsKey(key)
                    .containsEntry(key, (String) expectedValue);
        }
    }
}
