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
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaintenanceRegressionTest {
    private static final Schema SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    @TempDir Path directory;

    @Test
    void rewritesPartitionsLargerThanTheDefaultPerRunBudget() throws Exception {
        try (HadoopCatalog catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
            AppendFiles append = table.newAppend();
            // Planning only needs manifest metadata, so no 12 GiB data files are written.
            for (int i = 0; i < 96; i++) {
                append.appendFile(
                        DataFiles.builder(table.spec())
                                .withPath(table.location() + "/data/file-" + i + ".parquet")
                                .withFileSizeInBytes(128L * 1024 * 1024)
                                .withRecordCount(1)
                                .build());
            }
            append.commit();
            Map<String, String> conf = options("rewrite-data-files");
            StreamGraph graph = graph(conf);
            try (OneInputStreamOperatorTestHarness<Trigger, Object> harness =
                    harness(graph, "RDF Planner")) {
                harness.open();
                harness.processElement(new StreamRecord<>(Trigger.create(1, 0), 1));
                assertThat(harness.extractOutputValues()).isNotEmpty();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "expire-snapshots,false",
        "expire-snapshots,true",
        "delete-orphan-files,false",
        "delete-orphan-files,true"
    })
    void closesDeletionCatalogForExistingAndCdcCreatedTables(
            String task, boolean existsAtSubmission) throws Exception {
        try (HadoopCatalog catalog = catalog()) {
            Table table =
                    existsAtSubmission
                            ? catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA)
                            : null;
            StreamGraph graph = graph(options(task));
            if (table == null) {
                table = catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
            }
            Path orphan = directory.resolve("sales/orders/orphan.txt");
            Files.write(orphan, new byte[] {1});
            TrackingCatalog.opened = TrackingCatalog.closed = 0;
            try (OneInputStreamOperatorTestHarness<String, Object> harness =
                    harness(
                            graph,
                            task.equals("expire-snapshots") ? "Delete file" : "Delete File")) {
                harness.open();
                harness.processElement(new StreamRecord<>(orphan.toString()));
                assertThat(Files.exists(orphan)).isFalse();
            }
            assertThat(TrackingCatalog.opened).isPositive();
            assertThat(TrackingCatalog.closed).isEqualTo(TrackingCatalog.opened);
            assertThat(catalog.loadTable(TableIdentifier.of("sales", "orders")).uuid())
                    .isEqualTo(table.uuid());
        }
    }

    @Test
    void closesCatalogUsedToListMetadataFiles() throws Exception {
        StreamGraph graph = graph(options("delete-orphan-files"));
        try (HadoopCatalog catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
            table.newAppend()
                    .appendFile(
                            DataFiles.builder(table.spec())
                                    .withPath(table.location() + "/data/file.parquet")
                                    .withFileSizeInBytes(100)
                                    .withRecordCount(1)
                                    .build())
                    .commit();
            TrackingCatalog.opened = TrackingCatalog.closed = 0;
            try (OneInputStreamOperatorTestHarness<Trigger, String> harness =
                    harness(graph, "List metadata Files")) {
                harness.open();
                harness.processElement(new StreamRecord<>(Trigger.create(1, 0), 1));
                assertThat(harness.extractOutputValues()).isNotEmpty();
            }
            assertThat(TrackingCatalog.opened).isEqualTo(1);
            assertThat(TrackingCatalog.closed).isEqualTo(1);
        }
    }

    @Test
    void nativeFunctionsOwnIndependentCatalogsBeforeSerialization() throws Exception {
        try (HadoopCatalog catalog = catalog()) {
            Table table = catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
            table.newAppend()
                    .appendFile(
                            DataFiles.builder(table.spec())
                                    .withPath(table.location() + "/data/file.parquet")
                                    .withFileSizeInBytes(100)
                                    .withRecordCount(1)
                                    .build())
                    .commit();
        }
        StreamGraph graph = graph(options("delete-orphan-files"));
        TrackingCatalog.opened = TrackingCatalog.closed = 0;
        try (OneInputStreamOperatorTestHarness<Trigger, String> filesystem =
                harness(graph, "Filesystem Files")) {
            filesystem.open();
            try (OneInputStreamOperatorTestHarness<Trigger, String> metadata =
                    harness(graph, "List metadata Files")) {
                metadata.open();
                metadata.processElement(new StreamRecord<>(Trigger.create(1, 0), 1));
                filesystem.processElement(new StreamRecord<>(Trigger.create(1, 0), 1));
                assertThat(TrackingCatalog.opened).isEqualTo(2);
                assertThat(TrackingCatalog.closed).isZero();
            }
            assertThat(TrackingCatalog.closed).isEqualTo(1);
            filesystem.processElement(new StreamRecord<>(Trigger.create(2, 0), 2));
            assertThat(TrackingCatalog.opened).isEqualTo(2);
        }
        assertThat(TrackingCatalog.closed).isEqualTo(2);
    }

    @Test
    void closesCatalogUsedByRewriteCommitter() throws Exception {
        try (HadoopCatalog catalog = catalog()) {
            catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
            Map<String, String> properties = new HashMap<>();
            properties.put("warehouse", directory.toString());
            properties.put("catalog-impl", TrackingCatalog.class.getName());
            TableLoader loader =
                    TableLoader.fromCatalog(
                            CatalogLoader.custom(
                                    "test",
                                    properties,
                                    new org.apache.hadoop.conf.Configuration(),
                                    properties.get("catalog-impl")),
                            TableIdentifier.of("sales", "orders"));
            TrackingCatalog.opened = TrackingCatalog.closed = 0;
            try (OneInputStreamOperatorTestHarness<?, Trigger> harness =
                    new OneInputStreamOperatorTestHarness<>(
                            new ClosingRewriteCommitter(
                                    "sales.orders", "RewriteDataFiles", 0, loader))) {
                harness.open();
                assertThat(TrackingCatalog.opened).isEqualTo(1);
            }
            assertThat(TrackingCatalog.closed).isEqualTo(1);
        }
    }

    @Test
    void checksReadyTargetsWithLinearCatalogAccess() throws Exception {
        Map<String, String> conf = options("rewrite-data-files");
        List<String> tables = new ArrayList<>();
        try (HadoopCatalog catalog = catalog()) {
            for (int i = 0; i < 10; i++) {
                String table = "table_" + i;
                catalog.createTable(TableIdentifier.of("sales", table), SCHEMA);
                tables.add("sales." + table);
            }
            conf.put("sink.maintenance.tables", String.join(";", tables));
            StreamGraph graph = graph(conf);
            TrackingCatalog.loads = 0;
            TrackingCatalog.opened = TrackingCatalog.closed = 0;
            try (OneInputStreamOperatorTestHarness<Tuple2<String, TableChange>, Void> harness =
                    harness(graph, "Wait for CDC target tables")) {
                harness.open();
                for (int round = 0; round < 2; round++) {
                    for (String table : tables) {
                        harness.processElement(
                                new StreamRecord<>(
                                        Tuple2.of(table, TableChange.builder().build())));
                    }
                }
                for (String table : tables) {
                    assertThat(harness.getSideOutput(TargetReadiness.outputTag(table))).hasSize(2);
                }
            }
            assertThat(TrackingCatalog.loads).isEqualTo(tables.size());
            assertThat(TrackingCatalog.opened).isEqualTo(1);
            assertThat(TrackingCatalog.closed).isEqualTo(TrackingCatalog.opened);
        }
    }

    @Test
    void rejectsAliasesCreatedAfterSubmission() throws Exception {
        Map<String, String> conf = options("rewrite-data-files");
        conf.put("sink.maintenance.tables", "sales.orders;sales.alias");
        StreamGraph graph = graph(conf, AliasCatalog.class.getName());
        try (HadoopCatalog catalog = catalog()) {
            catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
            try (OneInputStreamOperatorTestHarness<Tuple2<String, TableChange>, Void> harness =
                    harness(graph, "Wait for CDC target tables")) {
                harness.open();
                harness.processElement(
                        new StreamRecord<>(
                                Tuple2.of("sales.orders", TableChange.builder().build())));
                assertThat(harness.getSideOutput(TargetReadiness.outputTag("sales.orders")))
                        .hasSize(1);
                assertThatThrownBy(
                                () ->
                                        harness.processElement(
                                                new StreamRecord<>(
                                                        Tuple2.of(
                                                                "sales.alias",
                                                                TableChange.builder().build()))))
                        .hasMessageContaining("refer to the same Iceberg table");
                assertThat(harness.getSideOutput(TargetReadiness.outputTag("sales.alias")))
                        .isNullOrEmpty();
            }
        }
    }

    @Test
    void missingTableDoesNotBlockReadyTargets() throws Exception {
        Map<String, String> conf = options("rewrite-data-files");
        conf.put("sink.maintenance.tables", "sales.orders;sales.pending");
        StreamGraph graph = graph(conf);
        try (HadoopCatalog catalog = catalog();
                OneInputStreamOperatorTestHarness<Tuple2<String, TableChange>, Void> harness =
                        harness(graph, "Wait for CDC target tables")) {
            harness.open();
            harness.processElement(
                    new StreamRecord<>(Tuple2.of("sales.pending", TableChange.builder().build())));
            assertThat(harness.getSideOutput(TargetReadiness.outputTag("sales.pending")))
                    .isNullOrEmpty();
            catalog.createTable(TableIdentifier.of("sales", "orders"), SCHEMA);
            harness.processElement(
                    new StreamRecord<>(Tuple2.of("sales.orders", TableChange.builder().build())));
            assertThat(harness.getSideOutput(TargetReadiness.outputTag("sales.orders"))).hasSize(1);
            catalog.createTable(TableIdentifier.of("sales", "pending"), SCHEMA);
            harness.processElement(
                    new StreamRecord<>(Tuple2.of("sales.pending", TableChange.builder().build())));
            assertThat(harness.getSideOutput(TargetReadiness.outputTag("sales.pending")))
                    .hasSize(1);
        }
    }

    /** Both configured names resolve to the same physical table. */
    public static class AliasCatalog extends TrackingCatalog {
        @Override
        public Table loadTable(TableIdentifier table) {
            return super.loadTable(TableIdentifier.of("sales", "orders"));
        }
    }

    private HadoopCatalog catalog() {
        return new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), directory.toString());
    }

    private Map<String, String> options(String task) {
        Map<String, String> conf = MaintenanceOptionsTest.validOptions();
        conf.put("sink.maintenance.expire-snapshots.enabled", "false");
        conf.put("sink.maintenance." + task + ".enabled", "true");
        conf.put("sink.maintenance.delete-batch-size", "1");
        return conf;
    }

    private StreamGraph graph(Map<String, String> conf) {
        return graph(conf, TrackingCatalog.class.getName());
    }

    private StreamGraph graph(Map<String, String> conf, String catalogClass) {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("warehouse", directory.toString());
        catalogOptions.put("catalog-impl", catalogClass);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        MaintenanceGraphAdapter adapter =
                TableMaintenanceTopology.append(
                        env,
                        catalogOptions,
                        Collections.emptyMap(),
                        MaintenanceOptions.fromConfiguration(Configuration.fromMap(conf)));
        StreamGraph graph = env.getStreamGraph();
        adapter.postProcessStreamGraph(graph);
        return graph;
    }

    @SuppressWarnings("unchecked")
    private static <I, O> OneInputStreamOperatorTestHarness<I, O> harness(
            StreamGraph graph, String prefix) throws Exception {
        StreamNode node =
                graph.getStreamNodes().stream()
                        .filter(n -> n.getOperatorName().startsWith(prefix))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        return new OneInputStreamOperatorTestHarness<>(
                (OneInputStreamOperator<I, O>)
                        ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator());
    }

    /** Counts physical catalog connections and table lookups, including runtime operator use. */
    public static class TrackingCatalog extends HadoopCatalog {
        static int opened;
        static int closed;
        static int loads;

        @Override
        public void initialize(String name, Map<String, String> properties) {
            super.initialize(name, properties);
            opened++;
        }

        @Override
        public Table loadTable(TableIdentifier table) {
            loads++;
            return super.loadTable(table);
        }

        @Override
        public void close() throws IOException {
            super.close();
            closed++;
        }
    }
}
