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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSink;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOptions;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance.MaintenanceOptions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Runs the real Iceberg maintenance tasks against CDC-written local tables. */
class TableMaintenanceExecutionTest {
    @TempDir Path directory;

    @Test
    @Timeout(180)
    void rewritesCdcDataExpiresSnapshotsAndDeletesOnlyOldOrphans() throws Exception {
        String warehouse = directory.resolve("warehouse").toString();
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        TableId tableId = TableId.parse("sales.orders");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        CreateTableEvent create = new CreateTableEvent(tableId, schema);
        new IcebergMetadataApplier(catalogOptions).applySchemaChange(create);
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        try (HadoopCatalog catalog =
                        new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse);
                IcebergWriter writer =
                        new IcebergWriter(
                                catalogOptions,
                                0,
                                0,
                                ZoneId.of("UTC"),
                                0,
                                UUID.randomUUID().toString(),
                                UUID.randomUUID().toString(),
                                Collections.emptyMap());
                IcebergCommitter committer =
                        new IcebergCommitter(catalogOptions, Collections.emptyMap())) {
            writer.write(create, null);
            for (int id = 1; id <= 6; id++) {
                writer.write(
                        DataChangeEvent.insertEvent(tableId, row(generator, id, "before")), null);
                commit(writer, committer);
            }
            writer.write(
                    DataChangeEvent.updateEvent(
                            tableId, row(generator, 1, "before"), row(generator, 1, "after")),
                    null);
            writer.write(DataChangeEvent.deleteEvent(tableId, row(generator, 2, "before")), null);
            commit(writer, committer);
            Table table = catalog.loadTable(TableIdentifier.of("sales", "orders"));
            List<String> expected = rows(table);
            assertThat(expected)
                    .containsExactlyInAnyOrder(
                            "1:after", "3:before", "4:before", "5:before", "6:before");
            Path oldOrphan = directory.resolve("warehouse/sales/orders/data/old-orphan.parquet");
            Path newOrphan = oldOrphan.resolveSibling("new-orphan.parquet");
            Files.write(oldOrphan, new byte[] {1});
            Files.write(newOrphan, new byte[] {2});
            Files.setLastModifiedTime(
                    oldOrphan, FileTime.from(Instant.now().minus(Duration.ofDays(10))));
            Configuration conf = new Configuration();
            conf.set(MaintenanceOptions.ENABLED, true);
            conf.set(MaintenanceOptions.TABLES, "sales.orders");
            conf.set(
                    MaintenanceOptions.JDBC_URI,
                    "jdbc:derby:memory:maintenance" + UUID.randomUUID() + ";create=true");
            conf.set(MaintenanceOptions.JDBC_INIT_LOCK_TABLES, true);
            conf.set(MaintenanceOptions.RATE_LIMIT, Duration.ofSeconds(1));
            conf.set(MaintenanceOptions.LOCK_CHECK_DELAY, Duration.ofMillis(100));
            conf.set(MaintenanceOptions.REWRITE_ENABLED, true);
            conf.set(MaintenanceOptions.REWRITE_COMMIT_COUNT, 1);
            conf.set(MaintenanceOptions.REWRITE_MIN_INPUT_FILES, 2);
            conf.set(MaintenanceOptions.EXPIRE_ENABLED, true);
            conf.set(MaintenanceOptions.EXPIRE_INTERVAL, Duration.ofSeconds(1));
            conf.set(MaintenanceOptions.EXPIRE_MAX_AGE, Duration.ofMillis(1));
            conf.set(MaintenanceOptions.EXPIRE_RETAIN_LAST, 2);
            conf.set(MaintenanceOptions.ORPHAN_ENABLED, true);
            conf.set(MaintenanceOptions.ORPHAN_INTERVAL, Duration.ofSeconds(1));
            org.apache.flink.configuration.Configuration flinkConf =
                    new org.apache.flink.configuration.Configuration();
            flinkConf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.createLocalEnvironment(2, flinkConf);
            env.enableCheckpointing(500);
            JobClient job =
                    startMaintenance(
                            env,
                            catalogOptions,
                            MaintenanceOptions.fromConfiguration(conf),
                            "CDC table maintenance test");
            String savepoint;
            long compactedSnapshot;
            try {
                await().atMost(Duration.ofSeconds(90))
                        .pollInterval(Duration.ofMillis(250))
                        .untilAsserted(
                                () -> {
                                    checkJobHealthy(job);
                                    table.refresh();
                                    assertThat(table.currentSnapshot().operation())
                                            .isEqualTo("replace");
                                    List<Long> snapshots = new ArrayList<>();
                                    table.snapshots()
                                            .forEach(
                                                    snapshot ->
                                                            snapshots.add(snapshot.snapshotId()));
                                    assertThat(snapshots).hasSize(2);
                                    assertThat(Files.exists(oldOrphan)).isFalse();
                                });
                assertThat(Files.exists(newOrphan)).isTrue();
                assertThat(rows(table)).containsExactlyInAnyOrderElementsOf(expected);
                compactedSnapshot = table.currentSnapshot().snapshotId();
                savepoint =
                        job.triggerSavepoint(
                                        directory.resolve("savepoints").toUri().toString(),
                                        SavepointFormatType.CANONICAL)
                                .get(30, TimeUnit.SECONDS);
            } finally {
                cancel(job);
            }
            // Restore the monitor, trigger counters, and recovery lock from a real savepoint.
            conf.set(MaintenanceOptions.JDBC_INIT_LOCK_TABLES, false);
            flinkConf.setString("execution.savepoint.path", savepoint);
            StreamExecutionEnvironment restoredEnv =
                    StreamExecutionEnvironment.createLocalEnvironment(2, flinkConf);
            restoredEnv.enableCheckpointing(500);
            JobClient restoredJob =
                    startMaintenance(
                            restoredEnv,
                            catalogOptions,
                            MaintenanceOptions.fromConfiguration(conf),
                            "Restored CDC table maintenance test");
            try {
                writer.write(
                        DataChangeEvent.insertEvent(tableId, row(generator, 7, "later")), null);
                commit(writer, committer);
                await().atMost(Duration.ofSeconds(60))
                        .untilAsserted(
                                () -> {
                                    checkJobHealthy(restoredJob);
                                    table.refresh();
                                    assertThat(table.currentSnapshot().snapshotId())
                                            .isNotEqualTo(compactedSnapshot);
                                    assertThat(table.currentSnapshot().operation())
                                            .isEqualTo("replace");
                                    assertThat(table.snapshots()).hasSize(2);
                                });
                assertThat(rows(table)).contains("7:later").hasSize(6);
            } finally {
                cancel(restoredJob);
            }
        }
    }

    @Test
    @Timeout(180)
    void restoresWhileWaitingThenMaintainsTheTableCreatedByCdc() throws Exception {
        String warehouse = directory.resolve("new-warehouse").toString();
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        Configuration conf = new Configuration();
        conf.set(MaintenanceOptions.ENABLED, true);
        conf.set(MaintenanceOptions.TABLES, "sales.orders;sales.pending");
        conf.set(
                MaintenanceOptions.JDBC_URI,
                "jdbc:derby:memory:newtable" + UUID.randomUUID() + ";create=true");
        conf.set(MaintenanceOptions.JDBC_INIT_LOCK_TABLES, true);
        conf.set(MaintenanceOptions.RATE_LIMIT, Duration.ofSeconds(1));
        conf.set(MaintenanceOptions.LOCK_CHECK_DELAY, Duration.ofMillis(100));
        conf.set(MaintenanceOptions.REWRITE_ENABLED, true);
        conf.set(MaintenanceOptions.REWRITE_INTERVAL, Duration.ofSeconds(1));
        conf.set(MaintenanceOptions.REWRITE_MIN_INPUT_FILES, 2);
        conf.set(MaintenanceOptions.EXPIRE_ENABLED, true);
        conf.set(MaintenanceOptions.EXPIRE_INTERVAL, Duration.ofSeconds(1));
        conf.set(MaintenanceOptions.EXPIRE_MAX_AGE, Duration.ofMillis(1));
        conf.set(MaintenanceOptions.EXPIRE_RETAIN_LAST, 2);
        conf.set(MaintenanceOptions.ORPHAN_ENABLED, true);
        conf.set(MaintenanceOptions.ORPHAN_INTERVAL, Duration.ofSeconds(1));
        org.apache.flink.configuration.Configuration flinkConf =
                new org.apache.flink.configuration.Configuration();
        flinkConf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(2, flinkConf);
        env.enableCheckpointing(500);
        JobClient waitingJob =
                startMaintenance(
                        env,
                        catalogOptions,
                        MaintenanceOptions.fromConfiguration(conf),
                        "Maintenance waiting for CDC tables");
        String savepoint;
        try {
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> waitingJob.getJobStatus().get() == JobStatus.RUNNING);
            savepoint = savepointWhenReady(waitingJob, directory.resolve("waiting-savepoints"));
            assertThat(Files.exists(directory.resolve("new-warehouse/sales/orders/metadata")))
                    .isFalse();
        } finally {
            cancel(waitingJob);
        }
        conf.set(MaintenanceOptions.JDBC_INIT_LOCK_TABLES, false);
        flinkConf.setString("execution.savepoint.path", savepoint);
        StreamExecutionEnvironment restoredEnv =
                StreamExecutionEnvironment.createLocalEnvironment(2, flinkConf);
        restoredEnv.enableCheckpointing(500);
        JobClient job =
                startMaintenance(
                        restoredEnv,
                        catalogOptions,
                        MaintenanceOptions.fromConfiguration(conf),
                        "Restored maintenance waiting for CDC tables");
        try (HadoopCatalog catalog =
                new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse)) {
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> job.getJobStatus().get() == JobStatus.RUNNING);
            // A savepoint with both targets absent proves startup and checkpointing do not wait on
            // DDL.
            savepointWhenReady(job, directory.resolve("still-waiting-savepoints"));
            TableId tableId = TableId.parse("sales.orders");
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT().notNull())
                            .physicalColumn("name", DataTypes.STRING())
                            .primaryKey("id")
                            .build();
            CreateTableEvent create = new CreateTableEvent(tableId, schema);
            new IcebergMetadataApplier(catalogOptions).applySchemaChange(create);
            BinaryRecordDataGenerator generator =
                    new BinaryRecordDataGenerator(
                            schema.getColumnDataTypes().toArray(new DataType[0]));
            try (IcebergWriter writer =
                            new IcebergWriter(
                                    catalogOptions,
                                    0,
                                    0,
                                    ZoneId.of("UTC"),
                                    0,
                                    UUID.randomUUID().toString(),
                                    UUID.randomUUID().toString(),
                                    Collections.emptyMap());
                    IcebergCommitter committer =
                            new IcebergCommitter(catalogOptions, Collections.emptyMap())) {
                writer.write(create, null);
                for (int id = 1; id <= 6; id++) {
                    writer.write(
                            DataChangeEvent.insertEvent(
                                    tableId, row(generator, id, "created-by-cdc")),
                            null);
                    commit(writer, committer);
                }
            }
            Table table = catalog.loadTable(TableIdentifier.of("sales", "orders"));
            Path oldOrphan =
                    directory.resolve("new-warehouse/sales/orders/data/old-orphan.parquet");
            Files.write(oldOrphan, new byte[] {1});
            Files.setLastModifiedTime(
                    oldOrphan, FileTime.from(Instant.now().minus(Duration.ofDays(10))));
            await().atMost(Duration.ofSeconds(90))
                    .untilAsserted(
                            () -> {
                                checkJobHealthy(job);
                                table.refresh();
                                assertThat(table.currentSnapshot().operation())
                                        .isEqualTo("replace");
                                assertThat(table.snapshots()).hasSize(2);
                                assertThat(Files.exists(oldOrphan)).isFalse();
                            });
            assertThat(rows(table)).hasSize(6).contains("1:created-by-cdc", "6:created-by-cdc");
            assertThat(catalog.tableExists(TableIdentifier.of("sales", "pending"))).isFalse();
            assertThat(job.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
        } finally {
            cancel(job);
        }
    }

    private static String savepointWhenReady(JobClient job, Path path) throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> result =
                new java.util.concurrent.atomic.AtomicReference<>();
        await().atMost(Duration.ofSeconds(60))
                .until(
                        () -> {
                            checkJobHealthy(job);
                            try {
                                result.set(
                                        job.triggerSavepoint(
                                                        path.toUri().toString(),
                                                        SavepointFormatType.CANONICAL)
                                                .get(30, TimeUnit.SECONDS));
                                return true;
                            } catch (java.util.concurrent.ExecutionException e) {
                                if (e.getCause()
                                                instanceof
                                                org.apache.flink.runtime.checkpoint
                                                        .CheckpointException
                                        && ((org.apache.flink.runtime.checkpoint
                                                                        .CheckpointException)
                                                                e.getCause())
                                                        .getCheckpointFailureReason()
                                                == org.apache.flink.runtime.checkpoint
                                                        .CheckpointFailureReason
                                                        .NOT_ALL_REQUIRED_TASKS_RUNNING) {
                                    return false;
                                }
                                throw e;
                            }
                        });
        return result.get();
    }

    private static JobClient startMaintenance(
            StreamExecutionEnvironment env,
            Map<String, String> catalogOptions,
            MaintenanceOptions options,
            String jobName)
            throws Exception {
        IcebergDataSink dataSink =
                new IcebergDataSink(
                        catalogOptions,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        ZoneId.of("UTC"),
                        "schema",
                        CompactionOptions.builder().enabled(false).build(),
                        "maintenance-test-",
                        Collections.emptyMap(),
                        options);
        // Expanding the real sink builds maintenance inside Flink's StreamGraph generation.
        env.fromCollection(Collections.emptyList(), new EventTypeInfo())
                .uid("empty-cdc-source")
                .slotSharingGroup(options.get(MaintenanceOptions.SLOT_SHARING_GROUP))
                .sinkTo(((FlinkSinkProvider) dataSink.getEventSinkProvider()).getSink())
                .uid("cdc-iceberg-sink")
                .slotSharingGroup(options.get(MaintenanceOptions.SLOT_SHARING_GROUP));
        StreamGraph graph = env.getStreamGraph();
        graph.setJobName(jobName);
        dataSink.postProcessStreamGraph(graph);
        return env.executeAsync(graph);
    }

    private static void checkJobHealthy(JobClient job) throws Exception {
        java.util.concurrent.CompletableFuture<org.apache.flink.api.common.JobExecutionResult>
                result = job.getJobExecutionResult();
        if (result.isDone()) {
            result.get(10, TimeUnit.SECONDS);
        }
    }

    private static void cancel(JobClient job) throws Exception {
        try {
            job.cancel().get(30, TimeUnit.SECONDS);
        } catch (IllegalStateException e) {
            // Preserve the original job failure when the local executor has already shut down.
            if (!e.getMessage().contains("MiniCluster")) {
                throw e;
            }
        }
    }

    private static RecordData row(BinaryRecordDataGenerator generator, int id, String value) {
        return generator.generate(new Object[] {id, BinaryStringData.fromString(value)});
    }

    private static void commit(IcebergWriter writer, IcebergCommitter committer) throws Exception {
        committer.commit(
                writer.prepareCommit().stream()
                        .map(IcebergWriterTest.MockCommitRequestImpl::new)
                        .collect(Collectors.toList()));
    }

    private static List<String> rows(Table table) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                result.add(record.getField("id") + ":" + record.getField("name"));
            }
        }
        return result;
    }
}
