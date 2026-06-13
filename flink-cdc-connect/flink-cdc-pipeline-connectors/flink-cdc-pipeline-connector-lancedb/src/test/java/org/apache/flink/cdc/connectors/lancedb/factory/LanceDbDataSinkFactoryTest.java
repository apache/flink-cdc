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

package org.apache.flink.cdc.connectors.lancedb.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSink;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkConfig;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link LanceDbDataSinkFactory}. */
class LanceDbDataSinkFactoryTest {

    @Test
    void testCreateDataSink() throws Exception {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("lancedb", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(LanceDbDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("root.path", "/tmp/lancedb")
                                .put("table.path.mapping", "inventory.products:/tmp/products.lance")
                                .put("sink.changelog-mode", "append-with-metadata")
                                .put("sink.flush.max-rows", "10")
                                .put("sink.batch.max-rows-per-commit", "5")
                                .put("sink.flush.interval", "5s")
                                .put("sink.max-retries", "2")
                                .put("sink.retry.backoff", "100ms")
                                .put("write.max-rows-per-file", "1024")
                                .put("write.max-rows-per-group", "128")
                                .put("write.max-bytes-per-file", "1048576")
                                .put("storage.options.region", "us-east-1")
                                .build());

        DataSink dataSink = createDataSink(sinkFactory, conf);

        Assertions.assertThat(dataSink).isInstanceOf(LanceDbDataSink.class);
        LanceDbDataSinkConfig config = extractConfig(dataSink);
        Assertions.assertThat(config.getRootPath()).isEqualTo("/tmp/lancedb");
        Assertions.assertThat(config.getChangelogMode()).isEqualTo("append-with-metadata");
        Assertions.assertThat(config.getFlushMaxRows()).isEqualTo(10);
        Assertions.assertThat(config.getMaxRowsPerCommit()).isEqualTo(5);
        Assertions.assertThat(config.getStorageOptions()).containsEntry("region", "us-east-1");
    }

    @Test
    void testRejectInvalidChangelogMode() {
        Map<String, String> options = validOptions();
        options.put("sink.changelog-mode", "upsert");

        Assertions.assertThatThrownBy(
                        () ->
                                createDataSink(
                                        new LanceDbDataSinkFactory(),
                                        Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.changelog-mode");
    }

    @Test
    void testRejectInvalidMapping() {
        Map<String, String> options = validOptions();
        options.put("table.path.mapping", "inventory.products");

        Assertions.assertThatThrownBy(
                        () ->
                                createDataSink(
                                        new LanceDbDataSinkFactory(),
                                        Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sourceTable:targetPath");
    }

    @Test
    void testRejectDuplicateTargetPathMapping() {
        Map<String, String> options = validOptions();
        options.put(
                "table.path.mapping",
                "inventory.products:/tmp/products.lance;inventory.products_b:/tmp/products.lance");

        Assertions.assertThatThrownBy(
                        () ->
                                createDataSink(
                                        new LanceDbDataSinkFactory(),
                                        Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("same Lance dataset path");
    }

    @Test
    void testRejectInvalidRowsPerCommit() {
        Map<String, String> options = validOptions();
        options.put("sink.batch.max-rows-per-commit", "0");

        Assertions.assertThatThrownBy(
                        () ->
                                createDataSink(
                                        new LanceDbDataSinkFactory(),
                                        Configuration.fromMap(options)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.batch.max-rows-per-commit");
    }

    private static Map<String, String> validOptions() {
        return new HashMap<>(ImmutableMap.of("root.path", "/tmp/lancedb"));
    }

    private static DataSink createDataSink(DataSinkFactory sinkFactory, Configuration conf) {
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        conf, conf, Thread.currentThread().getContextClassLoader()));
    }

    private static LanceDbDataSinkConfig extractConfig(DataSink dataSink) throws Exception {
        Field field = LanceDbDataSink.class.getDeclaredField("config");
        field.setAccessible(true);
        return (LanceDbDataSinkConfig) field.get(dataSink);
    }
}
