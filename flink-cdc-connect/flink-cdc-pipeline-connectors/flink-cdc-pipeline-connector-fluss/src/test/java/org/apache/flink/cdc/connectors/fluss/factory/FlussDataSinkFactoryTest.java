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

package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.ForwardHashFunctionProvider;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSink;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.SinkPartitioningStrategy;
import org.apache.flink.cdc.connectors.fluss.sink.FlussHashFunctionProvider;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussDataSinkFactory}. */
class FlussDataSinkFactoryTest {
    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);

        Configuration conf = createValidConfiguration();
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(FlussDataSink.class);
        Assertions.assertThat(dataSink.getDataChangeEventHashFunctionProvider())
                .isInstanceOf(FlussHashFunctionProvider.class);
    }

    @Test
    void testCreateDataSinkWithForwardPartitioningStrategy() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Configuration conf = createValidConfiguration();
        conf.set(FlussDataSinkOptions.SINK_PARTITIONING_STRATEGY, SinkPartitioningStrategy.FORWARD);

        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));

        Assertions.assertThat(dataSink.getDataChangeEventHashFunctionProvider())
                .isInstanceOf(ForwardHashFunctionProvider.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "unsupported_key",
                "properties.unsupported.option",
                "properties.paimon",
                "properties.paimonx.option",
                "properties.iceberg",
                "properties.icebergx.option",
                "properties.icerberg.option",
                "paimon.file.format",
                "iceberg.write.format.default"
            })
    void testUnsupportedOption(String option) {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                                .put(option, "unsupported_value")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'fluss'.\n\n"
                                + "Unsupported options:\n\n"
                                + option);
    }

    @ParameterizedTest
    @CsvSource({
        "paimon.file.format, orc",
        "paimon.deletion-vectors.enabled, true",
        "paimon.unknown-option, unknown-value",
        "iceberg.write.format.default, parquet",
        "iceberg.unknown-option, unknown-value",
        "hudi.unknown-option, unknown-value",
        "lance.unknown-option, unknown-value"
    })
    void testDataLakeProperties(String option, String value) {
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                                .put("properties.table.datalake.enabled", "true")
                                .put("properties.table.datalake.freshness", "30s")
                                .put("properties." + option, value)
                                .put("properties.client.writer.acks", "all")
                                .build());

        DataSink dataSink =
                new FlussDataSinkFactory()
                        .createDataSink(
                                new FactoryHelper.DefaultContext(
                                        conf,
                                        conf,
                                        Thread.currentThread().getContextClassLoader()));

        Map<String, String> expectedTableProperties =
                ImmutableMap.of(
                        "table.datalake.enabled",
                        "true",
                        "table.datalake.freshness",
                        "30s",
                        option,
                        value);
        assertThat(dataSink.getMetadataApplier())
                .extracting("tableProperties")
                .isEqualTo(expectedTableProperties);
        assertThat(dataSink)
                .extracting("flussClientConfig")
                .isEqualTo(
                        org.apache.fluss.config.Configuration.fromMap(
                                ImmutableMap.of(
                                        "bootstrap.servers", "localhost:9123",
                                        "client.writer.acks", "all")));
    }

    @Test
    void testDataLakePropertiesRequireBootstrapServers() {
        Configuration conf =
                Configuration.fromMap(ImmutableMap.of("properties.paimon.file.format", "orc"));

        assertThatThrownBy(
                        () ->
                                new FlussDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        conf,
                                                        conf,
                                                        Thread.currentThread()
                                                                .getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Missing required options are:\n\nbootstrap.servers");
    }

    @Test
    void testPrefixRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                                .put("properties.table.option1", "value1")
                                .put("properties.client.option1", "value1")
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(FlussDataSink.class);
    }

    @Test
    void testWrongBucketKeyAndBucketNum() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);
        assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(
                                                        ImmutableMap.<String, String>builder()
                                                                .put(
                                                                        FlussDataSinkOptions
                                                                                .BOOTSTRAP_SERVERS
                                                                                .key(),
                                                                        "localhost:9123")
                                                                .put(
                                                                        FlussDataSinkOptions
                                                                                .BUCKET_KEY
                                                                                .key(),
                                                                        "database1.table1;a,b")
                                                                .build()),
                                                new Configuration(),
                                                Thread.currentThread().getContextClassLoader())))
                .hasMessageContaining("Invalid bucket key configuration: database1.table1;a,b");
        assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(
                                                        ImmutableMap.<String, String>builder()
                                                                .put(
                                                                        FlussDataSinkOptions
                                                                                .BOOTSTRAP_SERVERS
                                                                                .key(),
                                                                        "localhost:9123")
                                                                .put(
                                                                        FlussDataSinkOptions
                                                                                .BUCKET_NUMBER
                                                                                .key(),
                                                                        "database1.table1: 11a")
                                                                .build()),
                                                new Configuration(),
                                                Thread.currentThread().getContextClassLoader())))
                .hasMessageContaining("Invalid bucket number configuration: database1.table1: 11a");
    }

    private Configuration createValidConfiguration() {
        return Configuration.fromMap(
                ImmutableMap.<String, String>builder()
                        .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                        .put(FlussDataSinkOptions.BUCKET_KEY.key(), "database1.table1:a,b")
                        .put(FlussDataSinkOptions.BUCKET_NUMBER.key(), "database1.table1:2")
                        .build());
    }
}
