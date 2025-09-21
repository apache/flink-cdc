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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.OperatorUidGenerator;
import org.apache.flink.cdc.composer.flink.translator.PartitioningTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkFactory;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkOptions;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonMetadataApplier;
import org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketAssignOperator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommitRequestImpl;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.options.Options;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;
import static org.apache.flink.cdc.common.types.DataTypes.INT;
import static org.apache.flink.cdc.common.types.DataTypes.STRING;
import static org.apache.flink.cdc.common.types.DataTypes.VARCHAR;
import static org.apache.flink.configuration.ConfigConstants.DEFAULT_PARALLELISM;

/** An ITCase for {@link PaimonWriter} and {@link PaimonCommitter}. */
public class PaimonSinkITCase {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private Options catalogOptions;

    private TableEnvironment tEnv;

    private StreamExecutionEnvironment env;

    private String warehouse;

    private final TableId table1 = TableId.tableId("test", "table1");
    private final TableId table2 = TableId.tableId("test", "table2");

    private static int checkpointId = 1;

    public static final String TEST_DATABASE = "test";
    private static final String HADOOP_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hadoop-conf-dir"))
                    .getPath();

    private static final String HIVE_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hive-conf-dir"))
                    .getPath();

    private void initialize(String metastore)
            throws Catalog.DatabaseNotEmptyException, Catalog.DatabaseNotExistException {
        env =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setRuntimeMode(RuntimeExecutionMode.BATCH)
                        .setParallelism(DEFAULT_PARALLELISM);
        tEnv = StreamTableEnvironment.create(env);
        warehouse = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions = new Options();
        catalogOptions.setString("metastore", metastore);
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        if ("hive".equals(metastore)) {
            catalogOptions.setString("hadoop-conf-dir", HADOOP_CONF_DIR);
            catalogOptions.setString("hive-conf-dir", HIVE_CONF_DIR);
            tEnv.executeSql(
                    String.format(
                            "CREATE CATALOG paimon_catalog WITH ("
                                    + "'type'='paimon', "
                                    + "'warehouse'='%s', "
                                    + "'metastore'='hive', "
                                    + "'hadoop-conf-dir'='%s', "
                                    + "'hive-conf-dir'='%s', "
                                    + "'cache-enabled'='false'"
                                    + ")",
                            warehouse, HADOOP_CONF_DIR, HIVE_CONF_DIR));
        } else {
            tEnv.executeSql(
                    String.format(
                            "CREATE CATALOG paimon_catalog WITH ('type'='paimon', 'warehouse'='%s', 'cache-enabled'='false')",
                            warehouse));
        }
        FlinkCatalogFactory.createPaimonCatalog(catalogOptions)
                .dropDatabase(TEST_DATABASE, true, true);
    }

    private List<Event> createTestEvents(boolean enableDeleteVectors) throws SchemaEvolveException {
        return createTestEvents(enableDeleteVectors, false, true);
    }

    private List<Event> createTestEvents(
            boolean enableDeleteVectors, boolean appendOnly, boolean enabledBucketKey)
            throws SchemaEvolveException {
        return createTestEvents(enableDeleteVectors, appendOnly, enabledBucketKey, null);
    }

    private List<Event> createTestEvents(
            boolean enableDeleteVectors,
            boolean appendOnly,
            boolean enabledBucketKey,
            SchemaChange schemaChange)
            throws SchemaEvolveException {
        List<Event> testEvents = new ArrayList<>();
        Schema.Builder builder = Schema.newBuilder();
        if (!appendOnly) {
            builder.primaryKey("col1");
        } else if (enabledBucketKey) {
            builder.option("bucket-key", "col1");
            builder.option("bucket", "5");
        }
        // create table
        Schema schema =
                builder.physicalColumn("col1", STRING().notNull())
                        .physicalColumn("col2", STRING())
                        .option("deletion-vectors.enabled", String.valueOf(enableDeleteVectors))
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        testEvents.add(createTableEvent);
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        if (schemaChange != null) {
            metadataApplier.applySchemaChange(
                    new CreateTableEvent(table1, generateRandomSchema(schema, schemaChange)));
        } else {
            metadataApplier.applySchemaChange(createTableEvent);
        }

        // insert
        testEvents.add(
                generateInsert(
                        table1, Arrays.asList(Tuple2.of(STRING(), "1"), Tuple2.of(STRING(), "1"))));
        testEvents.add(
                generateInsert(
                        table1, Arrays.asList(Tuple2.of(STRING(), "2"), Tuple2.of(STRING(), "2"))));
        return testEvents;
    }

    private Schema generateRandomSchema(Schema schema, SchemaChange schemaChange) {
        Schema.Builder builder = new Schema.Builder();
        switch (schemaChange) {
            case ADD_COLUMN:
                {
                    builder.physicalColumn("col1", STRING().notNull())
                            .physicalColumn("col2", STRING())
                            .physicalColumn("op_ts", INT());
                    break;
                }
            case REMOVE_COLUMN:
                {
                    builder.physicalColumn("col1", STRING().notNull());
                    break;
                }
            case REORDER_COLUMN:
                {
                    builder.physicalColumn("col2", STRING())
                            .physicalColumn("col1", STRING().notNull());
                    break;
                }
            case MODIFY_COLUMN:
                {
                    builder.physicalColumn("col1", STRING().notNull())
                            .physicalColumn("col2", VARCHAR(10));
                    break;
                }
        }
        return schema.copy(builder.build().getColumns());
    }

    @ParameterizedTest
    @CsvSource({"filesystem, true", "filesystem, false", "hive, true", "hive, false"})
    public void testSinkWithDataChange(String metastore, boolean enableDeleteVector)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // insert
        writeAndCommit(
                writer, committer, createTestEvents(enableDeleteVector).toArray(new Event[0]));
        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"), Row.ofKind(RowKind.INSERT, "2", "2"));

        // delete
        writeAndCommit(
                writer,
                committer,
                generateDelete(
                        table1, Arrays.asList(Tuple2.of(STRING(), "1"), Tuple2.of(STRING(), "1"))));

        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "2", "2"));

        // update
        writeAndCommit(
                writer,
                committer,
                generateUpdate(
                        table1,
                        Arrays.asList(Tuple2.of(STRING(), "2"), Tuple2.of(STRING(), "2")),
                        Arrays.asList(Tuple2.of(STRING(), "2"), Tuple2.of(STRING(), "x"))));
        if (enableDeleteVector) {
            Assertions.assertThat(fetchResults(table1))
                    .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "2", "x"));
        } else {
            Assertions.assertThat(fetchResults(table1))
                    .containsExactlyInAnyOrder(Row.ofKind(RowKind.UPDATE_AFTER, "2", "x"));
        }

        if (enableDeleteVector) {
            Assertions.assertThat(fetchMaxSequenceNumber(table1.getTableName()))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1L), Row.ofKind(RowKind.INSERT, 4L));
        } else {
            Assertions.assertThat(fetchMaxSequenceNumber(table1.getTableName()))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1L),
                            Row.ofKind(RowKind.INSERT, 2L),
                            Row.ofKind(RowKind.INSERT, 4L));
        }
    }

    @ParameterizedTest
    @CsvSource({"filesystem, true", "hive, true", "filesystem, false", "hive, false"})
    public void testSinkWithDataChangeForAppendOnlyTable(String metastore, boolean enabledBucketKey)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // insert
        writeAndCommit(
                writer,
                committer,
                createTestEvents(false, true, enabledBucketKey).toArray(new Event[0]));
        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"), Row.ofKind(RowKind.INSERT, "2", "2"));

        // Insert
        writeAndCommit(
                writer,
                committer,
                generateInsert(
                        table1, Arrays.asList(Tuple2.of(STRING(), "3"), Tuple2.of(STRING(), "3"))));

        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"),
                        Row.ofKind(RowKind.INSERT, "2", "2"),
                        Row.ofKind(RowKind.INSERT, "3", "3"));

        // Delete
        writeAndCommit(
                writer,
                committer,
                generateDelete(
                        table1, Arrays.asList(Tuple2.of(STRING(), "3"), Tuple2.of(STRING(), "3"))));
        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"),
                        Row.ofKind(RowKind.INSERT, "2", "2"),
                        Row.ofKind(RowKind.INSERT, "3", "3"));

        // Update
        writeAndCommit(
                writer,
                committer,
                generateUpdate(
                        table1,
                        Arrays.asList(Tuple2.of(STRING(), "3"), Tuple2.of(STRING(), "3")),
                        Arrays.asList(Tuple2.of(STRING(), "3"), Tuple2.of(STRING(), "x"))));
        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"),
                        Row.ofKind(RowKind.INSERT, "2", "2"),
                        Row.ofKind(RowKind.INSERT, "3", "3"),
                        Row.ofKind(RowKind.INSERT, "3", "x"));
    }

    @ParameterizedTest
    @CsvSource({"filesystem, true", "filesystem, false", "hive, true", "hive, false"})
    public void testSinkWithSchemaChange(String metastore, boolean enableDeleteVector)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // 1. receive only DataChangeEvents during one checkpoint
        writeAndCommit(
                writer, committer, createTestEvents(enableDeleteVector).toArray(new Event[0]));
        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"), Row.ofKind(RowKind.INSERT, "2", "2"));

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        writeAndCommit(
                writer,
                committer,
                generateInsert(
                        table1, Arrays.asList(Tuple2.of(STRING(), "3"), Tuple2.of(STRING(), "3"))));

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(Column.physicalColumn("col3", STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        metadataApplier.applySchemaChange(addColumnEvent);
        writer.write(addColumnEvent, null);

        writeAndCommit(
                writer,
                committer,
                generateInsert(
                        table1,
                        Arrays.asList(
                                Tuple2.of(STRING(), "4"),
                                Tuple2.of(STRING(), "4"),
                                Tuple2.of(STRING(), "4"))));

        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1", null),
                        Row.ofKind(RowKind.INSERT, "2", "2", null),
                        Row.ofKind(RowKind.INSERT, "3", "3", null),
                        Row.ofKind(RowKind.INSERT, "4", "4", "4"));

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        writeAndCommit(
                writer,
                committer,
                generateInsert(
                        table1,
                        Arrays.asList(
                                Tuple2.of(STRING(), "5"),
                                Tuple2.of(STRING(), "5"),
                                Tuple2.of(STRING(), "5"))));

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("col2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        writer.write(dropColumnEvent, null);

        writeAndCommit(
                writer,
                committer,
                generateInsert(
                        table1, Arrays.asList(Tuple2.of(STRING(), "6"), Tuple2.of(STRING(), "6"))));

        List<Row> result = fetchResults(TableId.tableId("test", "`table1$files`"));
        Set<Row> deduplicated = new HashSet<>(result);
        Assertions.assertThat(result).hasSameSizeAs(deduplicated);

        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", null),
                        Row.ofKind(RowKind.INSERT, "2", null),
                        Row.ofKind(RowKind.INSERT, "3", null),
                        Row.ofKind(RowKind.INSERT, "4", "4"),
                        Row.ofKind(RowKind.INSERT, "5", "5"),
                        Row.ofKind(RowKind.INSERT, "6", "6"));

        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(table1);
        if (enableDeleteVector) {
            Assertions.assertThatThrownBy(
                            () -> metadataApplier.applySchemaChange(truncateTableEvent))
                    .isExactlyInstanceOf(SchemaEvolveException.class)
                    .cause()
                    .isExactlyInstanceOf(UnsupportedSchemaChangeEventException.class)
                    .extracting("exceptionMessage")
                    .isEqualTo("Unable to truncate a table with deletion vectors enabled.");
        } else {
            metadataApplier.applySchemaChange(truncateTableEvent);
            Assertions.assertThat(fetchResults(table1)).isEmpty();
        }

        DropTableEvent dropTableEvent = new DropTableEvent(table1);
        metadataApplier.applySchemaChange(dropTableEvent);
        Assertions.assertThatThrownBy(() -> fetchResults(table1))
                .hasRootCauseExactlyInstanceOf(SqlValidatorException.class)
                .hasRootCauseMessage("Object 'table1' not found within 'paimon_catalog.test'");
    }

    // Table structure change events that will be executed on existing tables.
    enum SchemaChange {
        ADD_COLUMN,
        REMOVE_COLUMN,
        REORDER_COLUMN,
        MODIFY_COLUMN;
    }

    @ParameterizedTest
    @EnumSource(SchemaChange.class)
    void testSinkWithSchemaChangeForExistedTable(SchemaChange schemaChange) throws Exception {
        initialize("filesystem");
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();
        BucketAssignOperator bucketAssignOperator =
                new BucketAssignOperator(catalogOptions, null, ZoneId.systemDefault(), null);
        SchemaEvolutionClient schemaEvolutionClient = Mockito.mock(SchemaEvolutionClient.class);
        Mockito.when(schemaEvolutionClient.getLatestEvolvedSchema(Mockito.any()))
                .thenReturn(Optional.empty());
        bucketAssignOperator.setSchemaEvolutionClient(schemaEvolutionClient);
        bucketAssignOperator.open(new TaskInfoImpl("test_TaskInfo", 1, 0, 1, 0));

        // 1. receive only DataChangeEvents during one checkpoint
        writeAndCommit(
                bucketAssignOperator,
                writer,
                committer,
                createTestEvents(false, false, true, schemaChange).toArray(new Event[0]));
        switch (schemaChange) {
            case ADD_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", "1", null),
                                    Row.ofKind(RowKind.INSERT, "2", "2", null));
                    break;
                }
            case REMOVE_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1"),
                                    Row.ofKind(RowKind.INSERT, "2"));
                    break;
                }
            case REORDER_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", "1"),
                                    Row.ofKind(RowKind.INSERT, "2", "2"));
                    break;
                }
            case MODIFY_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", "1"),
                                    Row.ofKind(RowKind.INSERT, "2", "2"));
                }
        }

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        writeAndCommit(
                bucketAssignOperator,
                writer,
                committer,
                generateInsert(
                        table1, Arrays.asList(Tuple2.of(STRING(), "3"), Tuple2.of(STRING(), "3"))));

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(Column.physicalColumn("col3", STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        metadataApplier.applySchemaChange(addColumnEvent);
        writer.write(bucketAssignOperator.convertSchemaChangeEvent(addColumnEvent), null);

        writeAndCommit(
                bucketAssignOperator,
                writer,
                committer,
                generateInsert(
                        table1,
                        Arrays.asList(
                                Tuple2.of(STRING(), "4"),
                                Tuple2.of(STRING(), "4"),
                                Tuple2.of(STRING(), "4"))));
        switch (schemaChange) {
            case ADD_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", "1", null, null),
                                    Row.ofKind(RowKind.INSERT, "2", "2", null, null),
                                    Row.ofKind(RowKind.INSERT, "3", "3", null, null),
                                    Row.ofKind(RowKind.INSERT, "4", "4", null, "4"));
                    break;
                }
            case REMOVE_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", null),
                                    Row.ofKind(RowKind.INSERT, "2", null),
                                    Row.ofKind(RowKind.INSERT, "3", null),
                                    Row.ofKind(RowKind.INSERT, "4", "4"));
                    break;
                }
            case REORDER_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", "1", null),
                                    Row.ofKind(RowKind.INSERT, "2", "2", null),
                                    Row.ofKind(RowKind.INSERT, "3", "3", null),
                                    Row.ofKind(RowKind.INSERT, "4", "4", "4"));
                    break;
                }
            case MODIFY_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", "1", null),
                                    Row.ofKind(RowKind.INSERT, "2", "2", null),
                                    Row.ofKind(RowKind.INSERT, "3", "3", null),
                                    Row.ofKind(RowKind.INSERT, "4", "4", "4"));
                }
        }

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        writeAndCommit(
                bucketAssignOperator,
                writer,
                committer,
                generateInsert(
                        table1,
                        Arrays.asList(
                                Tuple2.of(STRING(), "5"),
                                Tuple2.of(STRING(), "5"),
                                Tuple2.of(STRING(), "5"))));

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("col2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        writer.write(bucketAssignOperator.convertSchemaChangeEvent(dropColumnEvent), null);

        writeAndCommit(
                bucketAssignOperator,
                writer,
                committer,
                generateInsert(
                        table1, Arrays.asList(Tuple2.of(STRING(), "6"), Tuple2.of(STRING(), "6"))));

        List<Row> result = fetchResults(TableId.tableId("test", "`table1$files`"));
        Set<Row> deduplicated = new HashSet<>(result);
        Assertions.assertThat(result).hasSameSizeAs(deduplicated);

        switch (schemaChange) {
            case ADD_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", null, null),
                                    Row.ofKind(RowKind.INSERT, "2", null, null),
                                    Row.ofKind(RowKind.INSERT, "3", null, null),
                                    Row.ofKind(RowKind.INSERT, "4", null, "4"),
                                    Row.ofKind(RowKind.INSERT, "5", null, "5"),
                                    Row.ofKind(RowKind.INSERT, "6", null, "6"));
                    break;
                }
            case REMOVE_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", null),
                                    Row.ofKind(RowKind.INSERT, "2", null),
                                    Row.ofKind(RowKind.INSERT, "3", null),
                                    Row.ofKind(RowKind.INSERT, "4", "4"),
                                    Row.ofKind(RowKind.INSERT, "5", "5"),
                                    Row.ofKind(RowKind.INSERT, "6", "6"));
                    break;
                }
            case REORDER_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", null),
                                    Row.ofKind(RowKind.INSERT, "2", null),
                                    Row.ofKind(RowKind.INSERT, "3", null),
                                    Row.ofKind(RowKind.INSERT, "4", "4"),
                                    Row.ofKind(RowKind.INSERT, "5", "5"),
                                    Row.ofKind(RowKind.INSERT, "6", "6"));
                    break;
                }
            case MODIFY_COLUMN:
                {
                    Assertions.assertThat(fetchResults(table1))
                            .containsExactlyInAnyOrder(
                                    Row.ofKind(RowKind.INSERT, "1", null),
                                    Row.ofKind(RowKind.INSERT, "2", null),
                                    Row.ofKind(RowKind.INSERT, "3", null),
                                    Row.ofKind(RowKind.INSERT, "4", "4"),
                                    Row.ofKind(RowKind.INSERT, "5", "5"),
                                    Row.ofKind(RowKind.INSERT, "6", "6"));
                }
        }
    }

    @ParameterizedTest
    @CsvSource({"filesystem, true", "filesystem, false", "hive, true", "hive, false"})
    public void testSinkWithMultiTables(String metastore, boolean enableDeleteVector)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();
        List<Event> testEvents = createTestEvents(enableDeleteVector);
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", STRING())
                        .physicalColumn("col2", STRING())
                        .primaryKey("col1")
                        .option("bucket", "1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table2, schema);
        testEvents.add(createTableEvent);
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        metadataApplier.applySchemaChange(createTableEvent);
        // insert
        testEvents.add(
                generateInsert(
                        table2, Arrays.asList(Tuple2.of(STRING(), "1"), Tuple2.of(STRING(), "1"))));

        // insert
        writeAndCommit(writer, committer, testEvents.toArray(new Event[0]));

        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"), Row.ofKind(RowKind.INSERT, "2", "2"));
        Assertions.assertThat(fetchResults(table2))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "1", "1"));
    }

    private static void commit(
            PaimonWriter<Event> writer, Committer<MultiTableCommittable> committer)
            throws IOException, InterruptedException {
        Collection<Committer.CommitRequest<MultiTableCommittable>> commitRequests =
                writer.prepareCommit().stream()
                        .map(PaimonSinkITCase::correctCheckpointId)
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
    }

    private static void writeAndCommit(
            PaimonWriter<Event> writer, Committer<MultiTableCommittable> committer, Event... events)
            throws IOException, InterruptedException {
        for (Event event : events) {
            writer.write(event, null);
        }
        writer.flush(false);
        commit(writer, committer);
    }

    private static void writeAndCommit(
            BucketAssignOperator bucketAssignOperator,
            PaimonWriter<Event> writer,
            Committer<MultiTableCommittable> committer,
            Event... events)
            throws Exception {
        for (Event event : events) {
            if (event instanceof DataChangeEvent) {
                event = bucketAssignOperator.convertDataChangeEvent((DataChangeEvent) event);
            } else {
                event = bucketAssignOperator.convertSchemaChangeEvent((SchemaChangeEvent) event);
            }
            writer.write(event, null);
        }
        writer.flush(false);
        commit(writer, committer);
    }

    private List<Row> fetchResults(TableId tableId) {
        List<Row> results = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog." + tableId.toString())
                .execute()
                .collect()
                .forEachRemaining(results::add);
        return results;
    }

    private List<Row> fetchMaxSequenceNumber(String tableName) {
        List<Row> results = new ArrayList<>();
        tEnv.sqlQuery(
                        "select max_sequence_number from paimon_catalog.test.`"
                                + tableName
                                + "$files`")
                .execute()
                .collect()
                .forEachRemaining(results::add);
        return results;
    }

    private BinaryRecordData generate(List<Tuple2<DataType, Object>> elements) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(elements.stream().map(e -> e.f0).toArray(DataType[]::new)));
        return generator.generate(
                elements.stream()
                        .map(e -> e.f1)
                        .map(o -> o instanceof String ? BinaryStringData.fromString((String) o) : o)
                        .toArray(Object[]::new));
    }

    private DataChangeEvent generateInsert(
            TableId tableId, List<Tuple2<DataType, Object>> elements) {
        return DataChangeEvent.insertEvent(tableId, generate(elements));
    }

    private DataChangeEvent generateUpdate(
            TableId tableId,
            List<Tuple2<DataType, Object>> beforeElements,
            List<Tuple2<DataType, Object>> afterElements) {
        return DataChangeEvent.updateEvent(
                tableId, generate(beforeElements), generate(afterElements));
    }

    private DataChangeEvent generateDelete(
            TableId tableId, List<Tuple2<DataType, Object>> elements) {
        return DataChangeEvent.deleteEvent(tableId, generate(elements));
    }

    @ParameterizedTest
    @CsvSource({"filesystem, true", "filesystem, false", "hive, true", "hive, false"})
    public void testDuplicateCommitAfterRestore(String metastore, boolean enableDeleteVector)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions, new PaimonRecordEventSerializer(ZoneId.systemDefault()));
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // insert
        for (Event event : createTestEvents(enableDeleteVector)) {
            writer.write(event, null);
        }
        writer.flush(false);
        Collection<Committer.CommitRequest<MultiTableCommittable>> commitRequests =
                writer.prepareCommit().stream()
                        .map(PaimonSinkITCase::correctCheckpointId)
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);

        // We add a loop for restore 7 times
        for (int i = 2; i < 9; i++) {
            // We've two steps in checkpoint: 1. snapshotState(ckp); 2.
            // notifyCheckpointComplete(ckp).
            // It's possible that flink job will restore from a checkpoint with only step#1 finished
            // and
            // step#2 not.
            // CommitterOperator will try to re-commit recovered transactions.
            committer.commit(commitRequests);
            List<DataChangeEvent> events =
                    Collections.singletonList(
                            generateInsert(
                                    table1,
                                    Arrays.asList(
                                            Tuple2.of(STRING(), String.valueOf(i)),
                                            Tuple2.of(STRING(), String.valueOf(i)))));
            Assertions.assertThatCode(
                            () -> {
                                for (Event event : events) {
                                    writer.write(event, null);
                                }
                            })
                    .doesNotThrowAnyException();
            writer.flush(false);
            // Checkpoint id start from 1
            committer.commit(
                    writer.prepareCommit().stream()
                            .map(PaimonSinkITCase::correctCheckpointId)
                            .map(MockCommitRequestImpl::new)
                            .collect(Collectors.toList()));
        }

        List<Row> result = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog.test.`table1$snapshots`")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        if (enableDeleteVector) {
            // Each APPEND will trigger COMPACT once enable deletion-vectors.
            Assertions.assertThat(result).hasSize(16);
        } else {
            // 8 APPEND and 1 COMPACT
            Assertions.assertThat(result).hasSize(9);
        }
        result.clear();

        tEnv.sqlQuery("select * from paimon_catalog.test.`table1`")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "1", "1"),
                        Row.ofKind(RowKind.INSERT, "2", "2"),
                        Row.ofKind(RowKind.INSERT, "3", "3"),
                        Row.ofKind(RowKind.INSERT, "4", "4"),
                        Row.ofKind(RowKind.INSERT, "5", "5"),
                        Row.ofKind(RowKind.INSERT, "6", "6"),
                        Row.ofKind(RowKind.INSERT, "7", "7"),
                        Row.ofKind(RowKind.INSERT, "8", "8"));
    }

    @ParameterizedTest
    @CsvSource({"hive"})
    public void testSinkWithHiveMetastorePartitionedTable(String metastore) throws Exception {
        initialize(metastore);
        List<Event> testEvents = new ArrayList<>();
        Schema.Builder builder = Schema.newBuilder();
        builder.option("metastore.partitioned-table", "true");
        // create table
        Schema schema =
                builder.physicalColumn("pt", STRING())
                        .physicalColumn("pk", STRING())
                        .physicalColumn("name", STRING())
                        .primaryKey("pk")
                        .partitionKey("pt")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        testEvents.add(createTableEvent);
        testEvents.add(
                generateInsert(
                        table1,
                        Arrays.asList(
                                Tuple2.of(STRING(), "20250604"),
                                Tuple2.of(STRING(), "1"),
                                Tuple2.of(STRING(), "Alice"))));
        testEvents.add(
                generateInsert(
                        table1,
                        Arrays.asList(
                                Tuple2.of(STRING(), "20250604"),
                                Tuple2.of(STRING(), "2"),
                                Tuple2.of(STRING(), "Bob"))));

        runJobWithEvents(testEvents, true);

        Assertions.assertThat(fetchResults(table1))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "20250604", "1", "Alice"),
                        Row.ofKind(RowKind.INSERT, "20250604", "2", "Bob"));
    }

    private void runJobWithEvents(List<Event> events, boolean isBatchMode) throws Exception {
        DataStream<Event> stream = env.fromCollection(events, TypeInformation.of(Event.class));

        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("paimon", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(PaimonDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(PaimonDataSinkOptions.METASTORE.key(), "hive")
                                .put(PaimonDataSinkOptions.WAREHOUSE.key(), warehouse)
                                .put("catalog.properties.cache-enabled", "false")
                                .build());

        DataSink paimonSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));

        PartitioningTranslator partitioningTranslator = new PartitioningTranslator();

        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        SchemaChangeBehavior.EVOLVE,
                        "$$_schema_operator_$$",
                        DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT,
                        "UTC");

        stream =
                schemaOperatorTranslator.translateRegular(
                        stream,
                        DEFAULT_PARALLELISM,
                        isBatchMode,
                        paimonSink
                                .getMetadataApplier()
                                .setAcceptedSchemaEvolutionTypes(
                                        Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                                                .collect(Collectors.toSet())),
                        new ArrayList<>());
        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        stream =
                partitioningTranslator.translateRegular(
                        stream,
                        DEFAULT_PARALLELISM,
                        DEFAULT_PARALLELISM,
                        isBatchMode,
                        schemaOperatorIDGenerator.generate(),
                        paimonSink.getDataChangeEventHashFunctionProvider(DEFAULT_PARALLELISM),
                        new OperatorUidGenerator());

        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                new SinkDef("paimon", "Paimon Sink", conf),
                stream,
                paimonSink,
                isBatchMode,
                schemaOperatorIDGenerator.generate(),
                new OperatorUidGenerator());
        env.execute("runJobWithEvents").getJobExecutionResult();
    }

    private static MultiTableCommittable correctCheckpointId(MultiTableCommittable committable) {
        // update the right checkpointId for MultiTableCommittable
        return new MultiTableCommittable(
                committable.getDatabase(),
                committable.getTable(),
                checkpointId++,
                committable.kind(),
                committable.wrappedCommittable());
    }

    private static class MockCommitRequestImpl<CommT> extends CommitRequestImpl<CommT> {

        protected MockCommitRequestImpl(CommT committable) {
            super(
                    committable,
                    InternalSinkCommitterMetricGroup.wrap(
                            UnregisteredMetricsGroup.createOperatorMetricGroup()));
        }
    }

    private static class MockInitContext
            implements Sink.InitContext, SerializationSchema.InitializationContext {

        private MockInitContext() {}

        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        public MailboxExecutor getMailboxExecutor() {
            return null;
        }

        public ProcessingTimeService getProcessingTimeService() {
            return null;
        }

        public int getSubtaskId() {
            return 0;
        }

        public int getNumberOfParallelSubtasks() {
            return 0;
        }

        public int getAttemptNumber() {
            return 0;
        }

        public SinkWriterMetricGroup metricGroup() {
            return null;
        }

        public MetricGroup getMetricGroup() {
            return null;
        }

        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return this;
        }

        public boolean isObjectReuseEnabled() {
            return false;
        }

        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        public JobID getJobId() {
            return null;
        }

        @Override
        public JobInfo getJobInfo() {
            return null;
        }

        @Override
        public TaskInfo getTaskInfo() {
            return null;
        }
    }
}
