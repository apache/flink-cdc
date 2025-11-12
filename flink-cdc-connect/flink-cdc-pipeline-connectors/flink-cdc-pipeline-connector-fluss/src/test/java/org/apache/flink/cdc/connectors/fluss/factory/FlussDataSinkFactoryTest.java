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
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSink;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussDataSinkFactory}. */
public class FlussDataSinkFactoryTest {
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
    }

    @Test
    void testUnsupportedOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(FlussDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(FlussDataSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
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
                        "Unsupported options found for 'fluss'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
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
