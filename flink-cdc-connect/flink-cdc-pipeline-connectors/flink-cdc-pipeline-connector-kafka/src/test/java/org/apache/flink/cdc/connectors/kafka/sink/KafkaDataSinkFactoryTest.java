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

package org.apache.flink.cdc.connectors.kafka.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link KafkaDataSinkFactory}. */
public class KafkaDataSinkFactoryTest {

    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(KafkaDataSinkFactory.class);

        Configuration conf = Configuration.fromMap(ImmutableMap.<String, String>builder().build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(KafkaDataSink.class);
    }

    @Test
    void testUnsupportedOption() {

        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(KafkaDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("unsupported_key", "unsupported_value")
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
                        "Unsupported options found for 'kafka'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    void testUnsupportedFormatOption() {

        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(KafkaDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("value.format", "debezium-json")
                                .put("key.format", "csv")
                                .put("key.csv.field-delimiter", "|")
                                .put("debezium-json.encode.decimal-as-plain-number", "true")
                                .put("debezium-json.unsupported_key", "true")
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
                        "Unsupported options found for 'kafka'.\n\n"
                                + "Unsupported options:\n\n"
                                + "debezium-json.unsupported_key");
    }

    @Test
    void testPrefixRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(KafkaDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("properties.compression.type", "none")
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(KafkaDataSink.class);
    }
}
