/*
 * Licensed to the Apache Software Foundation (ASF) under one或多个
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional信息 regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express或 implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Tests for {@link ElasticsearchDataSinkFactory}. */
public class ElasticsearchDataSinkFactoryTest {

    private static final String ELASTICSEARCH_IDENTIFIER = "elasticsearch";

    /** Tests the creation of an Elasticsearch DataSink with valid configuration. */
    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory = getElasticsearchDataSinkFactory();

        Configuration conf = createValidConfiguration();
        DataSink dataSink = createDataSink(sinkFactory, conf);

        Assertions.assertThat(dataSink).isInstanceOf(ElasticsearchDataSink.class);
    }

    /** Tests the behavior when a required option is missing. */
    @Test
    void testLackRequiredOption() {
        DataSinkFactory sinkFactory = getElasticsearchDataSinkFactory();
        List<String> requiredKeys = getRequiredKeys(sinkFactory);
        for (String requiredKey : requiredKeys) {
            // 创建一个新的配置 Map，包含所有必需选项
            Map<String, String> options = new HashMap<>(createValidOptions());
            // 移除当前正在测试的必需选项
            options.remove(requiredKey);
            Configuration conf = Configuration.fromMap(options);
            // 打印日志以确保我们在测试缺少必需选项的情况
            System.out.println("Testing missing required option: " + requiredKey);

            // 添加创建 DataSink 对象的代码
            Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))

                    // Assertions to check for missing required option
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requiredKey));
        }
    }

    /** Tests the behavior when an unsupported option is provided. */
    @Test
    void testUnsupportedOption() {
        DataSinkFactory sinkFactory = getElasticsearchDataSinkFactory();

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("hosts", "localhost:9200")
                                .put("version", "7")
                                .put("unsupported_key", "unsupported_value")
                                .build());

        // 打印日志以确保我们在测试不受支持的选项
        System.out.println("Testing unsupported option");

        // Assertions to check for unsupported options
        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'elasticsearch'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    /**
     * Tests the creation of an Elasticsearch DataSink with valid configuration using prefixed
     * options.
     */
    @Test
    void testPrefixedRequiredOption() {
        DataSinkFactory sinkFactory = getElasticsearchDataSinkFactory();

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("hosts", "localhost:9200")
                                .put("batch.size.max", "500")
                                .put("inflight.requests.max", "5")
                                .put("version", "7") // Added version to the test configuration
                                .build());

        // 打印日志以确保我们在测试带前缀的必需选项
        System.out.println("Testing prefixed required option");

        DataSink dataSink = createDataSink(sinkFactory, conf);
        Assertions.assertThat(dataSink).isInstanceOf(ElasticsearchDataSink.class);
    }

    // Helper methods

    private DataSinkFactory getElasticsearchDataSinkFactory() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        ELASTICSEARCH_IDENTIFIER, DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(ElasticsearchDataSinkFactory.class);
        return sinkFactory;
    }

    private Configuration createValidConfiguration() {
        return Configuration.fromMap(
                ImmutableMap.<String, String>builder()
                        .put("hosts", "localhost:9200")
                        .put("version", "7") // Added version to the valid configuration
                        .build());
    }

    private Map<String, String> createValidOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("hosts", "localhost:9200");
        options.put("version", "7"); // Added version to the valid options
        return options;
    }

    private List<String> getRequiredKeys(DataSinkFactory sinkFactory) {
        return sinkFactory.requiredOptions().stream()
                .map(ConfigOption::key)
                .collect(Collectors.toList());
    }

    private DataSink createDataSink(DataSinkFactory sinkFactory, Configuration conf) {
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        conf, conf, Thread.currentThread().getContextClassLoader()));
    }
}
