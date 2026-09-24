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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.util.InstantiationUtil;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeferredTableLoaderTest {
    @TempDir Path directory;

    @Test
    void waitsWithoutInventingMetadataThenReadsTheCdcCreatedSchema() throws Exception {
        String location = directory.resolve("orders").toString();
        try (DeferredTableLoader loader =
                new DeferredTableLoader(
                        TableLoader.fromHadoopTable(
                                location, new org.apache.hadoop.conf.Configuration()),
                        "sales.orders")) {
            loader.open();
            Table table = loader.loadTable();
            assertThat(table.name()).isEqualTo("sales.orders");
            table.refresh();
            assertThat(table.currentSnapshot()).isNull();
            assertThat(loader.isReady()).isFalse();
            assertThatThrownBy(table::schema).isInstanceOf(NoSuchTableException.class);
            assertThat(Files.exists(directory.resolve("orders/metadata"))).isFalse();
            Table created =
                    new HadoopTables(new org.apache.hadoop.conf.Configuration())
                            .create(
                                    new Schema(
                                            Types.NestedField.required(
                                                    1, "actual_id", Types.LongType.get())),
                                    location);
            created.newAppend().commit();
            table.refresh();
            assertThat(table.schema().findField("actual_id")).isNotNull();
            assertThat(table.currentSnapshot().snapshotId())
                    .isEqualTo(created.currentSnapshot().snapshotId());
            created.updateSchema().addColumn("later_column", Types.StringType.get()).commit();
            table.refresh();
            assertThat(table.schema().findField("later_column")).isNotNull();
            assertThat(loader.isReady()).isTrue();
        }
    }

    @Test
    void serializesLoaderAndDeletionIoBeforeTheTableExists() throws Exception {
        String location = directory.resolve("orders").toString();
        try (DeferredTableLoader loader =
                        new DeferredTableLoader(
                                TableLoader.fromHadoopTable(
                                        location, new org.apache.hadoop.conf.Configuration()),
                                "sales.orders");
                TableLoader restored =
                        InstantiationUtil.clone(loader, getClass().getClassLoader());
                FileIO io =
                        InstantiationUtil.clone(
                                loader.loadTable().io(), getClass().getClassLoader())) {
            assertThat(restored.loadTable().currentSnapshot()).isNull();
            ((SupportsBulkOperations) io).deleteFiles(Collections.emptyList());
            assertThat(Files.exists(directory.resolve("orders/metadata"))).isFalse();
            Table created =
                    new HadoopTables(new org.apache.hadoop.conf.Configuration())
                            .create(
                                    new Schema(
                                            Types.NestedField.required(
                                                    1, "id", Types.LongType.get())),
                                    location);
            created.newAppend().commit();
            assertThat(restored.loadTable().currentSnapshot()).isNotNull();
            Path orphan = directory.resolve("orders/orphan");
            Files.write(orphan, new byte[] {1});
            ((SupportsBulkOperations) io).deleteFiles(Collections.singletonList(orphan.toString()));
            assertThat(Files.exists(orphan)).isFalse();
            assertThat(Files.exists(directory.resolve("orders/metadata"))).isTrue();
        }
    }

    @Test
    void reloadsMetadataAfterTheCachedSnapshotExpires() throws Exception {
        String location = directory.resolve("orders").toString();
        Table created =
                new HadoopTables(new org.apache.hadoop.conf.Configuration())
                        .create(
                                new Schema(
                                        Types.NestedField.required(1, "id", Types.LongType.get())),
                                location);
        String firstFile = location + "/data/first.parquet";
        String secondFile = location + "/data/second.parquet";
        created.newAppend()
                .appendFile(
                        DataFiles.builder(created.spec())
                                .withPath(firstFile)
                                .withFileSizeInBytes(100)
                                .withRecordCount(1)
                                .build())
                .commit();
        try (DeferredTableLoader loader =
                new DeferredTableLoader(
                        TableLoader.fromHadoopTable(
                                location, new org.apache.hadoop.conf.Configuration()),
                        "sales.orders")) {
            loader.open();
            Table cached = loader.loadTable();
            long expiredSnapshot = cached.currentSnapshot().snapshotId();
            String expiredManifestList = cached.currentSnapshot().manifestListLocation();
            created.newAppend()
                    .appendFile(
                            DataFiles.builder(created.spec())
                                    .withPath(secondFile)
                                    .withFileSizeInBytes(100)
                                    .withRecordCount(1)
                                    .build())
                    .commit();
            created.expireSnapshots().expireSnapshotId(expiredSnapshot).commit();
            assertThat(created.io().newInputFile(expiredManifestList).exists()).isFalse();

            Table planningTable = SerializableTable.copyOf(loader.loadTable());
            try (CloseableIterable<FileScanTask> tasks = planningTable.newScan().planFiles()) {
                assertThat(tasks)
                        .extracting(task -> task.file().path().toString())
                        .containsExactlyInAnyOrder(firstFile, secondFile);
            }
            assertThat(planningTable.currentSnapshot().snapshotId())
                    .isEqualTo(created.currentSnapshot().snapshotId());
        }
    }

    @Test
    void preservesFailuresOtherThanMissingTables() throws Exception {
        try (DeferredTableLoader loader =
                new DeferredTableLoader(new FailingLoader(), "sales.orders")) {
            assertThatThrownBy(loader::isReady).hasMessage("catalog unavailable");
        }
    }

    private static final class FailingLoader implements TableLoader {
        @Override
        public void open() {}

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public Table loadTable() {
            throw new IllegalStateException("catalog unavailable");
        }

        @Override
        public TableLoader clone() {
            return new FailingLoader();
        }

        @Override
        public void close() {}
    }
}
