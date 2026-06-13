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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.RootAllocator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;

/** Integration tests for LanceDB sink against local Lance datasets. */
class LanceDbSinkITCase {

    @TempDir Path tempDir;

    @BeforeEach
    void assumeLanceNativeAvailable() {
        Assumptions.assumeTrue(
                isLanceNativeAvailable(),
                "Lance native library is not available for this JVM architecture.");
    }

    @Test
    void testCreateDatasetWriteAndCountRows() throws Exception {
        LanceDbDataSinkConfig config = config(tempDir.toString(), "append-only");

        LanceDbMetadataApplier metadataApplier = new LanceDbMetadataApplier(config);
        metadataApplier.applySchemaChange(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA));
        metadataApplier.close();

        try (LanceDbEventWriter writer = new LanceDbEventWriter(config)) {
            writer.write(
                    new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);
            writer.write(
                    DataChangeEvent.insertEvent(
                            LanceDbTestUtils.TABLE_ID,
                            LanceDbTestUtils.record(1, "keyboard", 99.5D)),
                    null);
            writer.write(
                    DataChangeEvent.insertEvent(
                            LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.record(2, "mouse", 39.9D)),
                    null);
            writer.flush(false);
        }

        String datasetPath = tempDir.resolve("inventory_products.lance").toString();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset dataset = Dataset.open(datasetPath, allocator)) {
            Assertions.assertThat(dataset.countRows()).isEqualTo(2L);
            Assertions.assertThat(dataset.getSchema().findField("name")).isNotNull();
        }
    }

    @Test
    void testAppendWithMetadataWritesDeleteAsLog() throws Exception {
        LanceDbDataSinkConfig config = config(tempDir.toString(), "append-with-metadata");
        LanceDbMetadataApplier metadataApplier = new LanceDbMetadataApplier(config);
        metadataApplier.applySchemaChange(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA));
        metadataApplier.close();

        try (LanceDbEventWriter writer = new LanceDbEventWriter(config)) {
            writer.write(
                    new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA), null);
            writer.write(
                    DataChangeEvent.deleteEvent(
                            LanceDbTestUtils.TABLE_ID,
                            LanceDbTestUtils.record(1, "keyboard", 99.5D)),
                    null);
            writer.flush(false);
        }

        String datasetPath = tempDir.resolve("inventory_products.lance").toString();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset dataset = Dataset.open(datasetPath, allocator)) {
            Assertions.assertThat(dataset.countRows()).isEqualTo(1L);
            Assertions.assertThat(dataset.getSchema().findField("_cdc_deleted")).isNotNull();
        }
    }

    private LanceDbDataSinkConfig config(String rootPath, String changelogMode) {
        return new LanceDbDataSinkConfig(
                rootPath,
                Collections.emptyMap(),
                true,
                true,
                true,
                false,
                changelogMode,
                100,
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                1024,
                128,
                1024 * 1024L,
                true,
                "CREATE",
                ZoneId.of("UTC"),
                Collections.emptyMap());
    }

    private boolean isLanceNativeAvailable() {
        try {
            Class.forName("com.lancedb.lance.Dataset", true, getClass().getClassLoader());
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }
}
