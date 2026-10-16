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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaintenanceFileIOTest {
    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void requiresPrefixListingOnlyForOrphanCleanup(boolean orphanCleanup) throws Exception {
        try (HadoopCatalog catalog =
                new HadoopCatalog(
                        new org.apache.hadoop.conf.Configuration(), directory.toString())) {
            catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
        }
        CatalogLoader loader =
                CatalogLoader.custom(
                        "test",
                        Collections.singletonMap("warehouse", directory.toString()),
                        new org.apache.hadoop.conf.Configuration(),
                        BulkOnlyCatalog.class.getName());
        Map<String, String> values = MaintenanceOptionsTest.validOptions();
        values.put("sink.maintenance.delete-orphan-files.enabled", Boolean.toString(orphanCleanup));
        TargetReadiness readiness =
                new TargetReadiness(
                        loader,
                        MaintenanceOptions.fromConfiguration(Configuration.fromMap(values)));
        TableChange change = TableChange.builder().build();
        try (OneInputStreamOperatorTestHarness<Tuple2<String, TableChange>, Void> harness =
                new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(readiness))) {
            harness.open();
            StreamRecord<Tuple2<String, TableChange>> record =
                    new StreamRecord<>(Tuple2.of("sales.orders", change));
            if (orphanCleanup) {
                assertThatThrownBy(() -> harness.processElement(record))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("must support prefix listing");
            } else {
                harness.processElement(record);
                assertThat(harness.getSideOutput(TargetReadiness.outputTag("sales.orders")))
                        .extracting(StreamRecord::getValue)
                        .containsExactly(change);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void listsCandidatesUsingTheConfiguredFileSystem(boolean existsAtSubmission) throws Exception {
        String warehouse = "maintenance-test://" + directory;
        Map<String, String> hadoopOptions = new HashMap<>();
        hadoopOptions.put("fs.maintenance-test.impl", ConfiguredFileSystem.class.getName());
        hadoopOptions.put("fs.maintenance-test.impl.disable.cache", "true");
        org.apache.hadoop.conf.Configuration hadoopConf =
                new org.apache.hadoop.conf.Configuration();
        hadoopOptions.forEach(hadoopConf::set);
        try (HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse)) {
            TableIdentifier tableId = TableIdentifier.of("sales", "orders");
            if (existsAtSubmission) {
                catalog.createTable(tableId, SCHEMA);
            }
            Map<String, String> catalogOptions = new HashMap<>();
            catalogOptions.put("type", "hadoop");
            catalogOptions.put("warehouse", warehouse);
            Map<String, String> values = MaintenanceOptionsTest.validOptions();
            values.put("sink.maintenance.expire-snapshots.enabled", "false");
            values.put("sink.maintenance.delete-orphan-files.enabled", "true");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(1000);
            MaintenanceGraphAdapter adapter =
                    TableMaintenanceTopology.append(
                            env,
                            catalogOptions,
                            hadoopOptions,
                            MaintenanceOptions.fromConfiguration(Configuration.fromMap(values)));
            StreamGraph graph = env.getStreamGraph();
            adapter.postProcessStreamGraph(graph);
            if (!existsAtSubmission) {
                catalog.createTable(tableId, SCHEMA);
            }
            Path orphan = directory.resolve("sales/orders/orphan.parquet");
            Files.write(orphan, new byte[] {1});
            Files.setLastModifiedTime(orphan, FileTime.fromMillis(0));
            Path recent = directory.resolve("sales/orders/recent.parquet");
            Files.write(recent, new byte[] {2});
            StreamNode node =
                    graph.getStreamNodes().stream()
                            .filter(n -> n.getOperatorName().startsWith("Filesystem Files"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            @SuppressWarnings("unchecked")
            OneInputStreamOperator<Trigger, String> operator =
                    (OneInputStreamOperator<Trigger, String>)
                            ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
            try (OneInputStreamOperatorTestHarness<Trigger, String> harness =
                    new OneInputStreamOperatorTestHarness<>(operator)) {
                harness.open();
                long now = System.currentTimeMillis();
                harness.processElement(new StreamRecord<>(Trigger.create(now, 0), now));
                assertThat(harness.getSideOutput(DeleteOrphanFiles.ERROR_STREAM)).isNullOrEmpty();
                assertThat(harness.extractOutputValues())
                        .containsExactly("maintenance-test:" + orphan);
            }
        }
    }

    /** Exposes bulk deletion without the optional prefix-listing capability. */
    public static class BulkOnlyCatalog extends HadoopCatalog {
        @Override
        public Table loadTable(TableIdentifier identifier) {
            BaseTable table = (BaseTable) super.loadTable(identifier);
            FileIO bulkOnly =
                    (FileIO)
                            Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class<?>[] {SupportsBulkOperations.class},
                                    (proxy, method, args) -> method.invoke(table.io(), args));
            return new BaseTable(table.operations(), table.name(), table.reporter()) {
                @Override
                public FileIO io() {
                    return bulkOnly;
                }
            };
        }
    }

    /** Only available through the Hadoop options supplied to the CDC sink. */
    public static class ConfiguredFileSystem extends RawLocalFileSystem {
        @Override
        public URI getUri() {
            return URI.create("maintenance-test:///");
        }

        @Override
        public FileStatus[] listStatus(org.apache.hadoop.fs.Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            for (int i = 0; i < statuses.length; i++) {
                FileStatus status = statuses[i];
                // RawLocalFileStatus lazily reads permissions through a file-scheme URI.
                statuses[i] =
                        new FileStatus(
                                status.getLen(),
                                status.isDirectory(),
                                status.getReplication(),
                                status.getBlockSize(),
                                status.getModificationTime(),
                                status.getPath());
            }
            return statuses;
        }
    }
}
