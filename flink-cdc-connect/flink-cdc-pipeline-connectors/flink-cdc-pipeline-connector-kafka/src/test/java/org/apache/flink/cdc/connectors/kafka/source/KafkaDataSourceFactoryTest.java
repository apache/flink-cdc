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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link KafkaDataSourceFactory}. */
class KafkaDataSourceFactoryTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test-group";
    private static final String TOPIC = "test-topic";

    @Test
    void testCreateDataSource() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory).isInstanceOf(KafkaDataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(KafkaDataSourceOptions.TOPIC.key(), TOPIC)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "bootstrap.servers",
                                        BOOTSTRAP_SERVERS)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .build());

        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSource).isInstanceOf(KafkaDataSource.class);
    }

    @Test
    void testMissingTopicThrowsException() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "bootstrap.servers",
                                        BOOTSTRAP_SERVERS)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'topic' is required");
    }

    @Test
    void testMissingBootstrapServersThrowsException() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(KafkaDataSourceOptions.TOPIC.key(), TOPIC)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'properties.bootstrap.servers' is required");
    }

    @Test
    void testUnsupportedValueFormat() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(KafkaDataSourceOptions.TOPIC.key(), TOPIC)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "bootstrap.servers",
                                        BOOTSTRAP_SERVERS)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "debezium-json")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Could not parse value 'debezium-json'");
    }

    @Test
    void testInvalidStartupMode() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(KafkaDataSourceOptions.TOPIC.key(), TOPIC)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "bootstrap.servers",
                                        BOOTSTRAP_SERVERS)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "invalid-mode")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Could not parse value 'invalid-mode'");
    }

    @Test
    void testTimestampStartupModeWithoutTimestamp() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(KafkaDataSourceOptions.TOPIC.key(), TOPIC)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "bootstrap.servers",
                                        BOOTSTRAP_SERVERS)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "timestamp")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.startup.timestamp-millis");
    }

    @Test
    void testUnsupportedOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(KafkaDataSourceOptions.TOPIC.key(), TOPIC)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "bootstrap.servers",
                                        BOOTSTRAP_SERVERS)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .put("unsupported_key", "unsupported_value")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'kafka'");
    }

    @Test
    void testPropertiesPrefixOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(KafkaDataSourceOptions.TOPIC.key(), TOPIC)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "bootstrap.servers",
                                        BOOTSTRAP_SERVERS)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX + "group.id",
                                        GROUP_ID)
                                .put(
                                        KafkaDataSourceOptions.PROPERTIES_PREFIX
                                                + "auto.offset.reset",
                                        "earliest")
                                .build());

        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSource).isInstanceOf(KafkaDataSource.class);
    }
}
