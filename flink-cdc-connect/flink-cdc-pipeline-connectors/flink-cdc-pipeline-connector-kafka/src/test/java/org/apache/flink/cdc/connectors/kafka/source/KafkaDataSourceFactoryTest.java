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
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.table.api.ValidationException;

import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link KafkaDataSourceFactory}. */
class KafkaDataSourceFactoryTest {

    @Test
    void testFactoryDiscoveryAndOptions() {
        DataSourceFactory factory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);
        Map<String, String> options = new HashMap<>();
        options.put("topic", "orders-a, orders-b");
        options.put("group-id", "pipeline-group");
        options.put("properties.bootstrap.servers", "localhost:9092");
        options.put("properties.client.id", "pipeline-client");
        Configuration configuration = Configuration.fromMap(options);

        DataSource source =
                factory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                configuration,
                                configuration,
                                Thread.currentThread().getContextClassLoader()));

        Assertions.assertThat(source).isInstanceOf(KafkaDataSource.class);
        KafkaDataSource kafkaSource = (KafkaDataSource) source;
        Assertions.assertThat(kafkaSource.getTopics()).containsExactly("orders-a", "orders-b");
        Assertions.assertThat(kafkaSource.getProperties())
                .containsEntry("group.id", "pipeline-group")
                .containsEntry("client.id", "pipeline-client");
        Assertions.assertThat(source.isParallelMetadataSource()).isTrue();
    }

    @Test
    void testTopicPattern() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic-pattern", "orders-.*");

        KafkaDataSource source = (KafkaDataSource) createSource(options);

        Assertions.assertThat(source.getTopics()).isEmpty();
        Assertions.assertThat(source.getTopicPattern()).isEqualTo("orders-.*");
    }

    @Test
    void testTopicAndTopicPatternAreMutuallyExclusive() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders");
        options.put("topic-pattern", "orders-.*");

        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Exactly one of options 'topic' and 'topic-pattern'");

        options.remove("topic");
        options.remove("topic-pattern");
        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Exactly one of options 'topic' and 'topic-pattern'");
    }

    @Test
    void testTimestampStartupRequiresMillis() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders");
        options.put("scan.startup.mode", "timestamp");

        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.startup.timestamp-millis");

        options.put("scan.startup.timestamp-millis", "1710000000000");
        KafkaDataSource source = (KafkaDataSource) createSource(options);
        Assertions.assertThat(source.getStartupMode()).isEqualTo("timestamp");
        Assertions.assertThat(source.getStartupTimestampMillis()).isEqualTo(1710000000000L);
    }

    @Test
    void testSpecificOffsetsStartup() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders");
        options.put("scan.startup.mode", "specific-offsets");

        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.startup.specific-offsets");

        options.put("scan.startup.specific-offsets", "partition:0,offset:42");
        KafkaDataSource source = (KafkaDataSource) createSource(options);
        Assertions.assertThat(source.getStartupMode()).isEqualTo("specific-offsets");
        Assertions.assertThat(source.getSpecificOffsets())
                .containsEntry(new TopicPartition("orders", 0), 42L);
    }

    @Test
    void testSpecificOffsetsWithoutTopicFailsForMultipleTopics() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders-a,orders-b");
        options.put("scan.startup.mode", "specific-offsets");
        options.put("scan.startup.specific-offsets", "partition:0,offset:42");

        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("must include 'topic'");
    }

    @Test
    void testSpecificOffsetsWithoutTopicFailsForTopicPattern() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic-pattern", "orders-.*");
        options.put("scan.startup.mode", "specific-offsets");
        options.put("scan.startup.specific-offsets", "partition:0,offset:42");

        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("must include 'topic'");
    }

    @Test
    void testInvalidSpecificOffsetsFails() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders");
        options.put("scan.startup.mode", "specific-offsets");
        options.put("scan.startup.specific-offsets", "partition:0,offset:abc");

        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("offset");
    }

    @Test
    void testTablesOptionsArePreserved() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders");
        options.put("tables", "inventory.customers");
        options.put("tables.exclude", "inventory.orders");

        KafkaDataSource source = (KafkaDataSource) createSource(options);
        Assertions.assertThat(source.getTables()).isEqualTo("inventory.customers");
        Assertions.assertThat(source.getTablesExclude()).isEqualTo("inventory.orders");
        Assertions.assertThat(source.getValueFormat())
                .isEqualTo(JsonSerializationType.DEBEZIUM_JSON);
    }

    @Test
    void testValueFormatCanalJson() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders");
        options.put("value.format", "canal-json");

        KafkaDataSource source = (KafkaDataSource) createSource(options);
        Assertions.assertThat(source.getValueFormat()).isEqualTo(JsonSerializationType.CANAL_JSON);
    }

    @Test
    void testInvalidValueFormatFails() {
        Map<String, String> options = validConnectionOptions();
        options.put("topic", "orders");
        options.put("value.format", "avro");

        Assertions.assertThatThrownBy(() -> createSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value.format")
                .hasMessageContaining("avro");
    }

    @Test
    void testRequiresConnectionAndGroupProperties() {
        KafkaDataSourceFactory factory = new KafkaDataSourceFactory();
        Configuration configuration =
                Configuration.fromMap(Collections.singletonMap("topic", "orders"));

        Assertions.assertThatThrownBy(
                        () ->
                                factory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                configuration,
                                                configuration,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("properties.bootstrap.servers");
    }

    private static Map<String, String> validConnectionOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("group-id", "pipeline-group");
        options.put("properties.bootstrap.servers", "localhost:9092");
        return options;
    }

    private static DataSource createSource(Map<String, String> options) {
        Configuration configuration = Configuration.fromMap(options);
        return new KafkaDataSourceFactory()
                .createDataSource(
                        new FactoryHelper.DefaultContext(
                                configuration,
                                configuration,
                                Thread.currentThread().getContextClassLoader()));
    }
}
