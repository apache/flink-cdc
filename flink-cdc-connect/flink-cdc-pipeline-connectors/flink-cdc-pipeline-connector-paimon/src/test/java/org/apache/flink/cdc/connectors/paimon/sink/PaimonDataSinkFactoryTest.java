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

package org.apache.flink.cdc.connectors.paimon.sink;

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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Tests for {@link PaimonDataSinkFactory}. */
class PaimonDataSinkFactoryTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("paimon", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(PaimonDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(PaimonDataSinkOptions.METASTORE.key(), "filesystem")
                                .put(
                                        PaimonDataSinkOptions.WAREHOUSE.key(),
                                        new File(
                                                        temporaryFolder.toFile(),
                                                        UUID.randomUUID().toString())
                                                .toString())
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(PaimonDataSink.class);
    }

    @Test
    void testLackRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("paimon", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(PaimonDataSinkFactory.class);

        Map<String, String> options = new HashMap<>();
        options.put(PaimonDataSinkOptions.METASTORE.key(), "filesystem");
        options.put(
                PaimonDataSinkOptions.WAREHOUSE.key(),
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString());

        List<String> requireKeys =
                sinkFactory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        for (String requireKey : requireKeys) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Configuration conf = Configuration.fromMap(remainingOptions);

            Assertions.assertThatThrownBy(
                            () ->
                                    sinkFactory.createDataSink(
                                            new FactoryHelper.DefaultContext(
                                                    conf,
                                                    conf,
                                                    Thread.currentThread()
                                                            .getContextClassLoader())))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    void testUnsupportedOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("paimon", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(PaimonDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(PaimonDataSinkOptions.METASTORE.key(), "filesystem")
                                .put(
                                        PaimonDataSinkOptions.WAREHOUSE.key(),
                                        new File(
                                                        temporaryFolder.toFile(),
                                                        UUID.randomUUID().toString())
                                                .toString())
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
                        "Unsupported options found for 'paimon'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    void testPrefixRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("paimon", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(PaimonDataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(PaimonDataSinkOptions.METASTORE.key(), "filesystem")
                                .put(
                                        PaimonDataSinkOptions.WAREHOUSE.key(),
                                        new File(
                                                        temporaryFolder.toFile(),
                                                        UUID.randomUUID().toString())
                                                .toString())
                                .put("catalog.properties.uri", "")
                                .put("table.properties.bucket", "2")
                                .build());

        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(PaimonDataSink.class);
    }
}
