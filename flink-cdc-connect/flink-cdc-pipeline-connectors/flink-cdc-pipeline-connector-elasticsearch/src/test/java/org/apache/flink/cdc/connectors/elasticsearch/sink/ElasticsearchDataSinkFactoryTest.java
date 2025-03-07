/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Tests for {@link ElasticsearchDataSinkFactory}. */
class ElasticsearchDataSinkFactoryTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(ElasticsearchDataSinkFactoryTest.class);

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
            Map<String, String> options = new HashMap<>(createValidOptions());
            options.remove(requiredKey);
            Configuration conf = Configuration.fromMap(options);
            LOG.info("Testing missing required option: {}", requiredKey);

            Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
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

        LOG.info("Testing unsupported option");

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

        LOG.info("Testing prefixed required option");

        DataSink dataSink = createDataSink(sinkFactory, conf);
        Assertions.assertThat(dataSink).isInstanceOf(ElasticsearchDataSink.class);
    }

    /**
     * Test the `validateShardingSeparator` method with illegal sharding separators. This test
     * checks two scenarios: 1. Separators containing illegal characters. 2. A separator with
     * uppercase letters.
     */
    @Test
    void testIllegalShardingSeparator()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ElasticsearchDataSinkFactory sinkFactory =
                (ElasticsearchDataSinkFactory) getElasticsearchDataSinkFactory();
        Method method =
                ElasticsearchDataSinkFactory.class.getDeclaredMethod(
                        "validateShardingSeparator", String.class);
        method.setAccessible(true);

        // Test an array of invalid separators with illegal characters
        String[] invalidSeparators = {"*", " ", ">", "<", "|", "?", "\"", ",", "#", "\\"};
        for (String invalidSeparator : invalidSeparators) {
            Throwable thrown =
                    Assertions.catchThrowable(() -> method.invoke(sinkFactory, invalidSeparator));
            Assertions.assertThat(extractException(thrown))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "sharding.suffix.separator is malformed, elasticsearch index cannot include \\, /, *, ?, \", <, >, |, ` ` (space character), ,, #");
        }

        // Test a separator with uppercase letters
        Throwable thrown = Assertions.catchThrowable(() -> method.invoke(sinkFactory, "_TEST"));
        Assertions.assertThat(extractException(thrown))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "sharding.suffix.separator is malformed, elasticsearch index only support lowercase.");
    }

    // Helper methods

    /** Helper method to extract the actual cause from an InvocationTargetException. */
    private Throwable extractException(Throwable ex) {
        Assertions.assertThat(ex).isInstanceOf(InvocationTargetException.class);
        return ex.getCause();
    }

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
