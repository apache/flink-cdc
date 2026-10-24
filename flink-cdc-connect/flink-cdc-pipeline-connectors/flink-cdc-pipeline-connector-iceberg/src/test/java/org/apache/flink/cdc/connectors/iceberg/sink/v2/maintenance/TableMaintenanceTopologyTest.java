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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSink;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkFactory;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.IcebergSink;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.WriteResultWrapper;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableMaintenanceTopologyTest {
    @TempDir Path directory;

    @Test
    void attachesAllTasksThroughSinkProviderWithStableUniqueUids() throws Exception {
        Map<String, String> conf = MaintenanceOptionsTest.validOptions();
        // Iceberg's JDBC lock ID must stay below 100 characters even for long UID prefixes.
        conf.put("sink.maintenance.uid-prefix", String.join("", Collections.nCopies(100, "x")));
        conf.put("sink.maintenance.tables", "sales.users;sales.orders");
        conf.put("sink.maintenance.rewrite-data-files.enabled", "true");
        conf.put("sink.maintenance.delete-orphan-files.enabled", "true");
        String warehouse = directory.resolve("warehouse").toString();
        conf.put("catalog.properties.type", "hadoop");
        conf.put("catalog.properties.warehouse", warehouse);
        try (HadoopCatalog catalog =
                new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse)) {
            catalog.createNamespace(Namespace.of("sales"));
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
            catalog.createTable(TableIdentifier.of("sales", "orders"), schema);
            catalog.createTable(TableIdentifier.of("sales", "users"), schema);
        }
        StreamGraph graph = graph(conf);
        Set<String> first = uids(graph);
        assertThat(first).hasSize(graph.getStreamNodes().size());
        assertThat(
                        graph.getStreamNodes().stream()
                                .map(StreamNode::getOperatorName)
                                .filter(n -> n.contains("Monitor source"))
                                .count())
                .isEqualTo(2);
        assertThat(
                        graph.getStreamNodes().stream()
                                .map(StreamNode::getOperatorName)
                                .collect(Collectors.joining("\n")))
                .contains("Expire", "Rewrite", "Orphan");
        conf.put("sink.maintenance.tables", "sales.orders;sales.users");
        assertThat(uids(graph(conf))).isEqualTo(first);
        conf.remove("sink.maintenance.tables");
        assertThat(
                        uids(
                                graph(
                                        conf,
                                        java.util.Arrays.asList(
                                                TableId.parse("sales.users"),
                                                TableId.parse("sales.orders"),
                                                TableId.parse("sales.users"),
                                                TableId.parse("sales.orders")))))
                .isEqualTo(first);
        conf.put("sink.maintenance.tables", "sales.orders;sales.users");
        conf.put("sink.maintenance.rewrite-data-files.enabled", "false");
        Set<String> changedTasks = uids(graph(conf));
        changedTasks.retainAll(first);
        // The shared readiness gate has no task-indexed state.
        assertThat(changedTasks)
                .containsExactly("table-readiness-" + conf.get("sink.maintenance.uid-prefix"));
    }

    @Test
    void preservesNativeMaintenanceOperatorUids() throws Exception {
        Map<String, String> conf = MaintenanceOptionsTest.validOptions();
        conf.put("sink.maintenance.rewrite-data-files.enabled", "true");
        conf.put("sink.maintenance.delete-orphan-files.enabled", "true");
        conf.put("catalog.properties.type", "hadoop");
        conf.put("catalog.properties.warehouse", directory.toString());
        MaintenanceOptions options =
                MaintenanceOptions.fromConfiguration(Configuration.fromMap(conf));
        TableId tableId = TableId.parse("sales.orders");
        String location;
        try (HadoopCatalog catalog =
                new HadoopCatalog(
                        new org.apache.hadoop.conf.Configuration(), directory.toString())) {
            location =
                    catalog.createTable(
                                    TableIdentifier.of("sales", "orders"),
                                    new Schema(
                                            Types.NestedField.required(
                                                    1, "id", Types.LongType.get())))
                            .location();
        }
        StreamExecutionEnvironment nativeEnv = environment();
        String uid = TableMaintenanceTopology.uidSuffix(options, tableId);
        TableMaintenance.forTable(
                        nativeEnv,
                        TableLoader.fromHadoopTable(
                                location, new org.apache.hadoop.conf.Configuration()),
                        new JdbcLockFactory(
                                options.get(MaintenanceOptions.JDBC_URI),
                                "uid-test",
                                options.jdbcProperties()))
                .uidSuffix(uid)
                .add(RewriteDataFiles.builder().scheduleOnInterval(Duration.ofHours(1)))
                .add(ExpireSnapshots.builder().scheduleOnInterval(Duration.ofDays(1)))
                .add(
                        new RefreshingDeleteOrphanFilesBuilder()
                                .scheduleOnInterval(Duration.ofDays(7)))
                .append();
        Set<String> cdcUids = uids(graph(conf));
        cdcUids.remove("committed-results");
        cdcUids.remove("table-change-id-" + uid);
        cdcUids.remove("table-readiness-" + options.get(MaintenanceOptions.UID_PREFIX));
        assertThat(cdcUids).isEqualTo(uids(nativeEnv.getStreamGraph()));
    }

    @Test
    void buildsMaintenanceBeforeCdcCreatesTheTarget() {
        StreamExecutionEnvironment env = environment();
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", directory.toString());
        MaintenanceOptions options =
                MaintenanceOptions.fromConfiguration(
                        Configuration.fromMap(MaintenanceOptionsTest.validOptions()));
        TableMaintenanceTopology.append(env, catalogOptions, Collections.emptyMap(), options);
        assertThat(env.getTransformations()).isNotEmpty();
        assertThat(env.getStreamGraph().getStreamNodes()).isNotEmpty();
    }

    @Test
    void closesSubmissionCatalogsForExistingAndMissingTargets() throws Exception {
        try (HadoopCatalog catalog =
                new HadoopCatalog(
                        new org.apache.hadoop.conf.Configuration(), directory.toString())) {
            catalog.createTable(
                    TableIdentifier.of("sales", "orders"),
                    new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        }
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("warehouse", directory.toString());
        catalogOptions.put("catalog-impl", TrackingCatalog.class.getName());
        Map<String, String> conf = MaintenanceOptionsTest.validOptions();
        conf.put("sink.maintenance.tables", "sales.orders;sales.pending");
        conf.put("sink.maintenance.rewrite-data-files.enabled", "true");
        conf.put("sink.maintenance.delete-orphan-files.enabled", "true");
        TrackingCatalog.opened = 0;
        TrackingCatalog.closed = 0;
        StreamExecutionEnvironment env = environment();
        TableMaintenanceTopology.append(
                env,
                catalogOptions,
                Collections.emptyMap(),
                MaintenanceOptions.fromConfiguration(Configuration.fromMap(conf)));
        assertThat(env.getStreamGraph().getStreamNodes()).isNotEmpty();
        assertThat(TrackingCatalog.opened).isPositive();
        assertThat(TrackingCatalog.closed).isEqualTo(TrackingCatalog.opened);
    }

    /** Tracks catalog resources opened while submitting maintenance. */
    public static class TrackingCatalog extends HadoopCatalog {
        private static int opened;
        private static int closed;

        @Override
        public void initialize(String name, Map<String, String> properties) {
            super.initialize(name, properties);
            opened++;
        }

        @Override
        public void close() throws IOException {
            super.close();
            closed++;
        }
    }

    @Test
    void rejectsAliasesOfTheSameTableBeforeAddingOperators() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("warehouse", directory.toString());
        catalogOptions.put("catalog-impl", AliasCatalog.class.getName());
        try (HadoopCatalog catalog =
                new HadoopCatalog(
                        new org.apache.hadoop.conf.Configuration(), directory.toString())) {
            catalog.createTable(
                    TableIdentifier.of("sales", "orders"),
                    new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        }
        Map<String, String> conf = MaintenanceOptionsTest.validOptions();
        conf.put("sink.maintenance.tables", "sales.orders;sales.orders_alias");
        StreamExecutionEnvironment env = environment();
        assertThatThrownBy(
                        () ->
                                TableMaintenanceTopology.append(
                                        env,
                                        catalogOptions,
                                        Collections.emptyMap(),
                                        MaintenanceOptions.fromConfiguration(
                                                Configuration.fromMap(conf))))
                .hasRootCauseMessage(
                        "Maintenance targets sales.orders and sales.orders_alias refer to the same Iceberg table.");
        assertThat(env.getTransformations()).isEmpty();
    }

    /** Catalog fixture modeling multiple identifiers resolving to one physical table. */
    public static class AliasCatalog extends HadoopCatalog {
        @Override
        public Table loadTable(TableIdentifier identifier) {
            return super.loadTable(TableIdentifier.of("sales", "orders"));
        }
    }

    @Test
    void rejectsBatchModeAndMissingCheckpointsBeforeCatalogAccess() {
        MaintenanceOptions options =
                MaintenanceOptions.fromConfiguration(
                        Configuration.fromMap(MaintenanceOptionsTest.validOptions()));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(
                        () ->
                                TableMaintenanceTopology.append(
                                        env,
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        options))
                .hasMessageContaining("checkpointing");
        env.enableCheckpointing(1000);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        assertThatThrownBy(
                        () ->
                                TableMaintenanceTopology.append(
                                        env,
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        options))
                .hasMessageContaining("streaming");
    }

    @Test
    void rejectsUnresolvedDiscoveryBeforeCatalogAccess() {
        Map<String, String> conf = MaintenanceOptionsTest.validOptions();
        conf.remove("sink.maintenance.tables");
        StreamExecutionEnvironment env = environment();
        assertThatThrownBy(
                        () ->
                                TableMaintenanceTopology.append(
                                        env,
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        MaintenanceOptions.fromConfiguration(
                                                Configuration.fromMap(conf))))
                .hasMessageContaining("must be discovered");
        assertThat(env.getTransformations()).isEmpty();
    }

    @Test
    void disabledMaintenanceDoesNotLoadCatalogOrAddOperators() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableMaintenanceTopology.append(
                env, Collections.emptyMap(), Collections.emptyMap(), MaintenanceOptions.disabled());
        assertThat(env.getTransformations()).isEmpty();
    }

    private static StreamGraph graph(Map<String, String> values) {
        return graph(values, Collections.emptyList());
    }

    private static StreamGraph graph(
            Map<String, String> values, java.util.List<TableId> discovered) {
        Configuration conf = Configuration.fromMap(values);
        IcebergDataSink dataSink =
                (IcebergDataSink)
                        new IcebergDataSinkFactory()
                                .createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                new Configuration(),
                                                TableMaintenanceTopologyTest.class
                                                        .getClassLoader()));
        dataSink.discoverTargetTables(() -> discovered);
        IcebergSink sink =
                (IcebergSink) ((FlinkSinkProvider) dataSink.getEventSinkProvider()).getSink();
        StreamExecutionEnvironment env = environment();
        sink.addPostCommitTopology(
                env.<CommittableMessage<WriteResultWrapper>>fromCollection(
                                Collections.emptyList(),
                                CommittableMessageTypeInfo.of(sink::getCommittableSerializer))
                        .uid("committed-results"));
        StreamGraph graph = env.getStreamGraph();
        dataSink.postProcessStreamGraph(graph);
        return graph;
    }

    private static StreamExecutionEnvironment environment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(1000);
        return env;
    }

    private static Set<String> uids(StreamGraph graph) {
        return graph.getStreamNodes().stream()
                .map(StreamNode::getTransformationUID)
                .collect(Collectors.toSet());
    }
}
