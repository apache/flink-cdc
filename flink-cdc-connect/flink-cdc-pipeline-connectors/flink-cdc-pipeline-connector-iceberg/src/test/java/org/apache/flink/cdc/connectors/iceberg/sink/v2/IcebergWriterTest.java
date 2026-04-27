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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommitRequestImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.sink.SinkUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/** A test for {@link IcebergWriter}. */
public class IcebergWriterTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    public void testWriteWithSchemaEvolution() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);
        TableId tableId = TableId.parse("test.iceberg_table");

        // Create Table.
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.BIGINT().notNull(),
                                        "column for id",
                                        "AUTO_DECREMENT()")
                                .physicalColumn(
                                        "name",
                                        DataTypes.VARCHAR(255).notNull(),
                                        "column for name",
                                        "John Smith")
                                .physicalColumn("age", DataTypes.INT(), "column for age", "91")
                                .physicalColumn(
                                        "description",
                                        DataTypes.STRING(),
                                        "column for descriptions",
                                        "not important")
                                .physicalColumn(
                                        "bool_column",
                                        DataTypes.BOOLEAN(),
                                        "column for bool",
                                        "false")
                                .physicalColumn(
                                        "float_column",
                                        DataTypes.FLOAT(),
                                        "column for float",
                                        "1.0")
                                .physicalColumn(
                                        "double_column",
                                        DataTypes.DOUBLE(),
                                        "column for double",
                                        "1.0")
                                .physicalColumn(
                                        "decimal_column",
                                        DataTypes.DECIMAL(10, 2),
                                        "column for decimal",
                                        "1.0")
                                .physicalColumn(
                                        "date_column", DataTypes.DATE(), "column for date", "12345")
                                .primaryKey("id")
                                .partitionKey("id", "name")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);
        BinaryRecordDataGenerator binaryRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]));
        RecordData recordData =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("Mark"),
                            10,
                            BinaryStringData.fromString("test"),
                            true,
                            1.0f,
                            1.0d,
                            DecimalData.fromBigDecimal(new BigDecimal("1.0"), 10, 2),
                            9
                        });
        DataChangeEvent dataChangeEvent = DataChangeEvent.insertEvent(tableId, recordData);
        icebergWriter.write(dataChangeEvent, null);
        RecordData recordData2 =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            2L,
                            BinaryStringData.fromString("Bob"),
                            10,
                            BinaryStringData.fromString("test"),
                            true,
                            1.0f,
                            1.0d,
                            DecimalData.fromBigDecimal(new BigDecimal("1.0"), 10, 2),
                            9
                        });
        DataChangeEvent dataChangeEvent2 = DataChangeEvent.insertEvent(tableId, recordData2);
        icebergWriter.write(dataChangeEvent2, null);
        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        try (IcebergCommitter icebergCommitter =
                new IcebergCommitter(catalogOptions, new HashMap<>())) {
            icebergCommitter.commit(collection);
        }
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(
                        "1, Mark, 10, test, true, 1.0, 1.0, 1.00, 1970-01-10",
                        "2, Bob, 10, test, true, 1.0, 1.0, 1.00, 1970-01-10");

        // Add column.
        RecordData recordData3 =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            3L,
                            BinaryStringData.fromString("Bob"),
                            10,
                            BinaryStringData.fromString("test"),
                            true,
                            1.0f,
                            1.0d,
                            DecimalData.fromBigDecimal(new BigDecimal("1.0"), 10, 2),
                            9
                        });
        DataChangeEvent dataChangeEvent3 = DataChangeEvent.insertEvent(tableId, recordData3);
        icebergWriter.write(dataChangeEvent3, null);
        RecordData recordData4 =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            4L,
                            BinaryStringData.fromString("Bob"),
                            10,
                            BinaryStringData.fromString("test"),
                            true,
                            1.0f,
                            1.0d,
                            DecimalData.fromBigDecimal(new BigDecimal("1.0"), 10, 2),
                            9
                        });
        DataChangeEvent dataChangeEvent4 = DataChangeEvent.insertEvent(tableId, recordData4);
        icebergWriter.write(dataChangeEvent4, null);
        icebergWriter.flush(false);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "newStringColumn",
                                                DataTypes.STRING(),
                                                "comment for newStringColumn",
                                                "not important"))));
        icebergMetadataApplier.applySchemaChange(addColumnEvent);
        icebergWriter.write(addColumnEvent, null);
        binaryRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        SchemaUtils.applySchemaChangeEvent(
                                        createTableEvent.getSchema(), addColumnEvent)
                                .getColumnDataTypes()
                                .toArray(new DataType[0]));
        RecordData recordData5 =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            5L,
                            BinaryStringData.fromString("Mark"),
                            10,
                            BinaryStringData.fromString("test"),
                            true,
                            1.0f,
                            1.0d,
                            DecimalData.fromBigDecimal(new BigDecimal("1.0"), 10, 2),
                            9,
                            BinaryStringData.fromString("newStringColumn"),
                        });
        DataChangeEvent dataChangeEvent5 = DataChangeEvent.insertEvent(tableId, recordData5);
        icebergWriter.write(dataChangeEvent5, null);
        writeResults = icebergWriter.prepareCommit();
        collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        try (IcebergCommitter icebergCommitter =
                new IcebergCommitter(catalogOptions, new HashMap<>())) {
            icebergCommitter.commit(collection);
        }
        result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(
                        "1, Mark, 10, test, true, 1.0, 1.0, 1.00, 1970-01-10, null",
                        "2, Bob, 10, test, true, 1.0, 1.0, 1.00, 1970-01-10, null",
                        "3, Bob, 10, test, true, 1.0, 1.0, 1.00, 1970-01-10, null",
                        "4, Bob, 10, test, true, 1.0, 1.0, 1.00, 1970-01-10, null",
                        "5, Mark, 10, test, true, 1.0, 1.0, 1.00, 1970-01-10, newStringColumn");
    }

    @Test
    public void testWriteWithAllSupportedTypes() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        ZoneId pipelineZoneId = ZoneId.systemDefault();
        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        pipelineZoneId,
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);
        TableId tableId = TableId.parse("test.iceberg_table");

        // Create Table.
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn("char(5)", DataTypes.CHAR(5))
                                .physicalColumn("varchar(10)", DataTypes.VARCHAR(10))
                                .physicalColumn("string", DataTypes.STRING())
                                .physicalColumn("boolean", DataTypes.BOOLEAN())
                                .physicalColumn("binary(5)", DataTypes.BINARY(5))
                                .physicalColumn("varbinary(10)", DataTypes.BINARY(10))
                                .physicalColumn("decimal(10, 2)", DataTypes.DECIMAL(10, 2))
                                .physicalColumn("tinyint", DataTypes.TINYINT())
                                .physicalColumn("negative_tinyint", DataTypes.TINYINT())
                                .physicalColumn("smallint", DataTypes.SMALLINT())
                                .physicalColumn("negative_smallint", DataTypes.SMALLINT())
                                .physicalColumn("int", DataTypes.INT())
                                .physicalColumn("bigint", DataTypes.BIGINT())
                                .physicalColumn("float", DataTypes.FLOAT())
                                .physicalColumn("double", DataTypes.DOUBLE())
                                .physicalColumn("time", DataTypes.TIME())
                                .physicalColumn("date", DataTypes.DATE())
                                .physicalColumn("timestamp", DataTypes.TIMESTAMP())
                                .physicalColumn("timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                                .physicalColumn("timestamp_tz", DataTypes.TIMESTAMP_TZ())
                                .primaryKey("id")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);
        BinaryRecordDataGenerator dataGenerator =
                new BinaryRecordDataGenerator(
                        createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]));
        BinaryRecordData record1 =
                dataGenerator.generate(
                        new Object[] {
                            BinaryStringData.fromString("char"),
                            BinaryStringData.fromString("varchar"),
                            BinaryStringData.fromString("string"),
                            false,
                            new byte[] {1, 2, 3, 4, 5},
                            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
                            DecimalData.zero(10, 2),
                            (byte) 1,
                            (byte) -1,
                            (short) 2,
                            (short) -2,
                            12345,
                            12345L,
                            123.456f,
                            123456.789d,
                            12345,
                            12345,
                            TimestampData.fromTimestamp(Timestamp.valueOf("1970-01-01 00:00:00")),
                            LocalZonedTimestampData.fromInstant(Instant.ofEpochSecond(0)),
                            ZonedTimestampData.fromZonedDateTime(
                                    ZonedDateTime.ofInstant(
                                            Instant.ofEpochSecond(0), pipelineZoneId))
                        });
        DataChangeEvent dataChangeEvent = DataChangeEvent.insertEvent(tableId, record1);
        icebergWriter.write(dataChangeEvent, null);
        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        try (IcebergCommitter icebergCommitter =
                new IcebergCommitter(catalogOptions, new HashMap<>())) {
            icebergCommitter.commit(collection);
        }
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(
                        "char, varchar, string, false, [1,2,3,4,5,], [1,2,3,4,5,6,7,8,9,10,], 0.00, 1, -1, 2, -2, 12345, 12345, 123.456, 123456.789, 00:00:12.345, 2003-10-20, 1970-01-01T00:00, 1970-01-01T00:00Z, 1970-01-01T00:00Z");
    }

    @Test
    public void testWriteWithPartitionTypes() throws Exception {
        // all target value is from 1970-01-01 00:00:00 -> 1971-01-01 12:00:01
        Map<String, Expression> testcases = new HashMap<>();
        testcases.put("year(create_time)", Expressions.equal(Expressions.year("create_time"), 1));
        testcases.put(
                "month(create_time)", Expressions.equal(Expressions.month("create_time"), 12));
        testcases.put("day(create_time)", Expressions.equal(Expressions.day("create_time"), 365));
        testcases.put(
                "hour(create_time)",
                Expressions.equal(Expressions.hour("create_time"), 365 * 24 + 12));
        testcases.put("bucket[8](id)", Expressions.equal(Expressions.bucket("id", 8), 1));
        testcases.put("truncate[8](id)", Expressions.equal(Expressions.truncate("id", 8), 12344));

        for (Map.Entry<String, Expression> entry : testcases.entrySet()) {
            runTestPartitionWrite(entry.getKey(), entry.getValue());
        }
    }

    private void runTestPartitionWrite(String partitionKey, Expression expression)
            throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());

        TableId tableId = TableId.parse("test.iceberg_table");
        Map<TableId, List<String>> partitionMaps = new HashMap<>();
        partitionMaps.put(tableId, List.of(partitionKey));

        IcebergMetadataApplier icebergMetadataApplier =
                new IcebergMetadataApplier(catalogOptions, new HashMap<>(), partitionMaps);
        // Create Table.
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.BIGINT().notNull(),
                                        "column for id",
                                        "AUTO_DECREMENT()")
                                .physicalColumn(
                                        "create_time",
                                        DataTypes.TIMESTAMP().notNull(),
                                        "column for name",
                                        null)
                                .primaryKey("id")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);
        BinaryRecordDataGenerator binaryRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]));
        RecordData recordData =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            12345L,
                            TimestampData.fromTimestamp(Timestamp.valueOf("1971-01-01 12:00:01")),
                        });
        DataChangeEvent dataChangeEvent = DataChangeEvent.insertEvent(tableId, recordData);
        icebergWriter.write(dataChangeEvent, null);
        RecordData recordData2 =
                binaryRecordDataGenerator.generate(
                        new Object[] {
                            1L,
                            TimestampData.fromTimestamp(Timestamp.valueOf("2026-01-12 12:00:01")),
                        });
        DataChangeEvent dataChangeEvent2 = DataChangeEvent.insertEvent(tableId, recordData2);
        icebergWriter.write(dataChangeEvent2, null);
        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        try (IcebergCommitter icebergCommitter =
                new IcebergCommitter(catalogOptions, new HashMap<>())) {
            icebergCommitter.commit(collection);
        }

        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));

        // test filter files
        CloseableIterable<FileScanTask> tasks = table.newScan().filter(expression).planFiles();
        int fileCount = 0;
        for (FileScanTask task : tasks) {
            fileCount++;
            Assertions.assertThat(task.file().partition().toString()).contains("PartitionData{");
        }
        Assertions.assertThat(fileCount).isEqualTo(1);

        // test filter records
        List<String> result = fetchTableContent(catalog, tableId, expression);
        Assertions.assertThat(result.size()).isEqualTo(1);
        Assertions.assertThat(result).containsExactlyInAnyOrder("12345, 1971-01-01T12:00:01");

        result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void testWithRepeatCommit() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        ZoneId pipelineZoneId = ZoneId.systemDefault();
        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        pipelineZoneId,
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);
        TableId tableId = TableId.parse("test.iceberg_table");
        TableIdentifier tableIdentifier =
                TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName());
        // Create Table.
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.BIGINT().notNull(),
                                        "column for id",
                                        "AUTO_DECREMENT()")
                                .physicalColumn(
                                        "name", DataTypes.VARCHAR(100), "column for name", null)
                                .primaryKey("id")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);
        BinaryRecordDataGenerator dataGenerator =
                new BinaryRecordDataGenerator(
                        createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]));
        BinaryRecordData record1 =
                dataGenerator.generate(
                        new Object[] {
                            1L, BinaryStringData.fromString("char1"),
                        });
        DataChangeEvent dataChangeEvent = DataChangeEvent.insertEvent(tableId, record1);
        icebergWriter.write(dataChangeEvent, null);
        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        try (IcebergCommitter icebergCommitter =
                new IcebergCommitter(catalogOptions, new HashMap<>())) {
            icebergCommitter.commit(collection);
        }
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result.size()).isEqualTo(1);
        Assertions.assertThat(result).containsExactlyInAnyOrder("1, char1");
        Map<String, String> summary =
                catalog.loadTable(tableIdentifier).currentSnapshot().summary();
        Assertions.assertThat(summary.get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID)).isEqualTo("1");
        Assertions.assertThat(summary.get(SinkUtil.FLINK_JOB_ID)).isEqualTo(jobId);
        Assertions.assertThat(summary.get(SinkUtil.OPERATOR_ID)).isEqualTo(operatorId);

        // repeat commit with same committables, should not cause duplicate data.
        BinaryRecordData record2 =
                dataGenerator.generate(
                        new Object[] {
                            2L, BinaryStringData.fromString("char2"),
                        });
        DataChangeEvent dataChangeEvent2 = DataChangeEvent.insertEvent(tableId, record2);
        icebergWriter.write(dataChangeEvent2, null);
        writeResults = icebergWriter.prepareCommit();
        collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        try (IcebergCommitter icebergCommitter =
                new IcebergCommitter(catalogOptions, new HashMap<>())) {
            icebergCommitter.commit(collection);
            icebergCommitter.commit(collection);
        }
        summary = catalog.loadTable(tableIdentifier).currentSnapshot().summary();
        Assertions.assertThat(summary.get("total-data-files")).isEqualTo("2");
        Assertions.assertThat(summary.get("added-records")).isEqualTo("1");
        Assertions.assertThat(summary.get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID)).isEqualTo("2");
        Assertions.assertThat(summary.get(SinkUtil.FLINK_JOB_ID)).isEqualTo(jobId);
        Assertions.assertThat(summary.get(SinkUtil.OPERATOR_ID)).isEqualTo(operatorId);

        result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result.size()).isEqualTo(2);
        Assertions.assertThat(result).containsExactlyInAnyOrder("1, char1", "2, char2");
    }

    @Test
    public void testNoDuplicateWhenFlushSplitsSamePkUpdatesWithinCheckpoint() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        TableId tableId = TableId.parse("test.iceberg_table");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.BIGINT().notNull())
                                .physicalColumn("name", DataTypes.VARCHAR(100))
                                .primaryKey("id")
                                .build());
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]));

        RecordData recordA =
                generator.generate(new Object[] {1L, BinaryStringData.fromString("a")});
        RecordData recordB =
                generator.generate(new Object[] {1L, BinaryStringData.fromString("b")});

        icebergWriter.write(DataChangeEvent.insertEvent(tableId, recordA), null);
        // flush(false) is a no-op; both events reach the same writer so the position-delete
        // mechanism within the writer handles dedup correctly.
        icebergWriter.flush(false);
        icebergWriter.write(DataChangeEvent.updateEvent(tableId, recordA, recordB), null);

        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions, new HashMap<>());
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);

        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result).containsExactly("1, b");
    }

    /**
     * Verifies that same-PK updates split across a schema-change-triggered writer flush within one
     * checkpoint do not produce duplicate records.
     *
     * <p>Root cause: when a schema-change event forces {@code flushTableWriter}, the pre-change
     * writes land in one {@code WriteResultWrapper} (batch 0) and the post-change writes land in
     * another (batch 1). Committing both in a single Iceberg {@code RowDelta} would give all files
     * the same sequence number N, so the equality-delete file from batch 1 (seq=N) cannot delete
     * the data file from batch 0 (seq=N) — Iceberg equality-delete semantics require strictly lower
     * sequence numbers. The fix commits each batch as a separate sequential snapshot, giving batch
     * 1 a higher sequence number so its equality-delete correctly supersedes batch 0's data.
     */
    @Test
    public void testNoDuplicateWhenSchemaChangeFlushSplitsSamePkUpdates() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        TableId tableId = TableId.parse("test.iceberg_table");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, initialSchema);
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);

        BinaryRecordDataGenerator oldGenerator =
                new BinaryRecordDataGenerator(
                        initialSchema.getColumnDataTypes().toArray(new DataType[0]));

        // Insert (id=1, name="a") — goes into writer batch 0.
        RecordData recordA =
                oldGenerator.generate(new Object[] {1L, BinaryStringData.fromString("a")});
        icebergWriter.write(DataChangeEvent.insertEvent(tableId, recordA), null);

        // Schema change: AddColumn triggers flushTableWriter, completing batch 0.
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(addColumnEvent);
        icebergWriter.write(addColumnEvent, null);

        // Update (id=1) with the new schema — goes into writer batch 1.
        Schema newSchema = SchemaUtils.applySchemaChangeEvent(initialSchema, addColumnEvent);
        BinaryRecordDataGenerator newGenerator =
                new BinaryRecordDataGenerator(
                        newSchema.getColumnDataTypes().toArray(new DataType[0]));
        RecordData recordANew =
                newGenerator.generate(new Object[] {1L, BinaryStringData.fromString("a"), null});
        RecordData recordB =
                newGenerator.generate(new Object[] {1L, BinaryStringData.fromString("b"), null});
        icebergWriter.write(DataChangeEvent.updateEvent(tableId, recordANew, recordB), null);

        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions, new HashMap<>());
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);

        // Expect only (1, b, null) — batch 0's stale (1, a, null) must be deleted.
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result).containsExactly("1, b, null");
    }

    /**
     * Verifies idempotency when the committer crashes after committing batch 0 but before
     * committing batch 1 of the same checkpoint.
     *
     * <p>On retry, Flink re-delivers the full collection of committables for that checkpoint. The
     * committer must detect that batch 0's snapshot is already present in the table history (via
     * {@code flink.batch-index} and {@code flink.checkpoint-id} snapshot properties) and skip it,
     * then commit only batch 1. Without this skip, batch 0's files would be added a second time,
     * causing duplicate records.
     */
    @Test
    public void testRetryAfterPartialBatchCommit() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        TableId tableId = TableId.parse("test.iceberg_table");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, initialSchema);
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);

        BinaryRecordDataGenerator oldGenerator =
                new BinaryRecordDataGenerator(
                        initialSchema.getColumnDataTypes().toArray(new DataType[0]));
        RecordData recordA =
                oldGenerator.generate(new Object[] {1L, BinaryStringData.fromString("a")});
        icebergWriter.write(DataChangeEvent.insertEvent(tableId, recordA), null);

        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(addColumnEvent);
        icebergWriter.write(addColumnEvent, null);

        Schema newSchema = SchemaUtils.applySchemaChangeEvent(initialSchema, addColumnEvent);
        BinaryRecordDataGenerator newGenerator =
                new BinaryRecordDataGenerator(
                        newSchema.getColumnDataTypes().toArray(new DataType[0]));
        RecordData recordANew =
                newGenerator.generate(new Object[] {1L, BinaryStringData.fromString("a"), null});
        RecordData recordB =
                newGenerator.generate(new Object[] {1L, BinaryStringData.fromString("b"), null});
        icebergWriter.write(DataChangeEvent.updateEvent(tableId, recordANew, recordB), null);

        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        List<WriteResultWrapper> sortedBatches =
                writeResults.stream()
                        .sorted(Comparator.comparingInt(WriteResultWrapper::getBatchIndex))
                        .collect(Collectors.toList());
        Assertions.assertThat(sortedBatches).hasSize(2);
        Assertions.assertThat(sortedBatches.get(0).getBatchIndex()).isEqualTo(0);
        Assertions.assertThat(sortedBatches.get(1).getBatchIndex()).isEqualTo(1);

        // Simulate a partial commit: manually commit only batch 0 using the Iceberg API,
        // setting the intermediate batch properties but NOT MAX_COMMITTED_CHECKPOINT_ID.
        // This replicates the on-disk state left behind when the committer crashes mid-checkpoint.
        long checkpointId = sortedBatches.get(0).getCheckpointId();
        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));
        RowDelta partialDelta = table.newRowDelta();
        WriteResultWrapper batch0 = sortedBatches.get(0);
        if (batch0.getWriteResult().dataFiles() != null) {
            for (DataFile f : batch0.getWriteResult().dataFiles()) {
                partialDelta.addRows(f);
            }
        }
        if (batch0.getWriteResult().deleteFiles() != null) {
            for (DeleteFile f : batch0.getWriteResult().deleteFiles()) {
                partialDelta.addDeletes(f);
            }
        }
        partialDelta.set(SinkUtil.FLINK_JOB_ID, jobId);
        partialDelta.set(SinkUtil.OPERATOR_ID, operatorId);
        partialDelta.set(IcebergCommitter.FLINK_BATCH_INDEX, "0");
        partialDelta.set(IcebergCommitter.FLINK_CHECKPOINT_ID_PROP, String.valueOf(checkpointId));
        // Intentionally omitting MAX_COMMITTED_CHECKPOINT_ID — this is an incomplete checkpoint.
        partialDelta.commit();

        // Retry: Flink re-delivers all committables for the checkpoint.
        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions, new HashMap<>());
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);

        // Batch 0 must be skipped (its snapshot is already present); only batch 1 is committed.
        // Batch 1's eqDelete (higher sequence number) supersedes batch 0's data file.
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result).containsExactly("1, b, null");

        // Verify the final snapshot carries MAX_COMMITTED_CHECKPOINT_ID.
        Map<String, String> finalSummary =
                catalog.loadTable(
                                TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()))
                        .currentSnapshot()
                        .summary();
        Assertions.assertThat(finalSummary.get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID))
                .isEqualTo(String.valueOf(checkpointId));
    }

    /**
     * Verifies that two schema-change events within a single checkpoint (producing three batches)
     * do not cause duplicate records for the same primary key.
     *
     * <p>Timeline: INSERT(id=1,"a") → AddColumn1 flush → UPDATE(id=1,"b") → AddColumn2 flush →
     * UPDATE(id=1,"c") → commit. The three batches are committed as three sequential Iceberg
     * snapshots (seq=N, N+1, N+2), so each batch's equality-delete supersedes all earlier data.
     */
    @Test
    public void testNoDuplicateWithMultipleSchemaChangesInOneCheckpoint() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        TableId tableId = TableId.parse("test.iceberg_table");
        Schema schema0 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema0);
        icebergMetadataApplier.applySchemaChange(createTableEvent);
        icebergWriter.write(createTableEvent, null);

        // Batch 0: INSERT(id=1,"a")
        BinaryRecordDataGenerator gen0 =
                new BinaryRecordDataGenerator(
                        schema0.getColumnDataTypes().toArray(new DataType[0]));
        RecordData r0a = gen0.generate(new Object[] {1L, BinaryStringData.fromString("a")});
        icebergWriter.write(DataChangeEvent.insertEvent(tableId, r0a), null);

        // First schema change → flushTableWriter → batch 0 complete, batch index starts at 1.
        AddColumnEvent addCol1 =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra1", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(addCol1);
        icebergWriter.write(addCol1, null);

        // Batch 1: UPDATE(id=1,"a"→"b") with schema {id, name, extra1}
        Schema schema1 = SchemaUtils.applySchemaChangeEvent(schema0, addCol1);
        BinaryRecordDataGenerator gen1 =
                new BinaryRecordDataGenerator(
                        schema1.getColumnDataTypes().toArray(new DataType[0]));
        RecordData r1a = gen1.generate(new Object[] {1L, BinaryStringData.fromString("a"), null});
        RecordData r1b = gen1.generate(new Object[] {1L, BinaryStringData.fromString("b"), null});
        icebergWriter.write(DataChangeEvent.updateEvent(tableId, r1a, r1b), null);

        // Second schema change → flushTableWriter → batch 1 complete, batch index now at 2.
        AddColumnEvent addCol2 =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra2", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(addCol2);
        icebergWriter.write(addCol2, null);

        // Batch 2: UPDATE(id=1,"b"→"c") with schema {id, name, extra1, extra2}
        Schema schema2 = SchemaUtils.applySchemaChangeEvent(schema1, addCol2);
        BinaryRecordDataGenerator gen2 =
                new BinaryRecordDataGenerator(
                        schema2.getColumnDataTypes().toArray(new DataType[0]));
        RecordData r2b =
                gen2.generate(new Object[] {1L, BinaryStringData.fromString("b"), null, null});
        RecordData r2c =
                gen2.generate(new Object[] {1L, BinaryStringData.fromString("c"), null, null});
        icebergWriter.write(DataChangeEvent.updateEvent(tableId, r2b, r2c), null);

        // Verify three batches with indices 0, 1, 2 were produced.
        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();
        List<WriteResultWrapper> sortedBatches =
                writeResults.stream()
                        .sorted(Comparator.comparingInt(WriteResultWrapper::getBatchIndex))
                        .collect(Collectors.toList());
        Assertions.assertThat(sortedBatches).hasSize(3);
        Assertions.assertThat(sortedBatches.get(0).getBatchIndex()).isEqualTo(0);
        Assertions.assertThat(sortedBatches.get(1).getBatchIndex()).isEqualTo(1);
        Assertions.assertThat(sortedBatches.get(2).getBatchIndex()).isEqualTo(2);

        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions, new HashMap<>());
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);

        // Only the final value (id=1,"c") should survive; stale "a" and "b" must be deleted.
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result).containsExactly("1, c, null, null");
    }

    /**
     * Verifies that a schema-change flush on tableA does not affect tableB. TableB has no schema
     * change and commits as a single batch, while tableA's two batches are committed sequentially.
     * Both tables must contain exactly the correct final records.
     */
    @Test
    public void testSchemaChangeFlushDoesNotAffectOtherTable() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();
        IcebergWriter icebergWriter =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        TableId tableA = TableId.parse("test.table_a");
        TableId tableB = TableId.parse("test.table_b");

        Schema schemaA =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        Schema schemaB =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("value", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();

        icebergMetadataApplier.applySchemaChange(new CreateTableEvent(tableA, schemaA));
        icebergMetadataApplier.applySchemaChange(new CreateTableEvent(tableB, schemaB));
        icebergWriter.write(new CreateTableEvent(tableA, schemaA), null);
        icebergWriter.write(new CreateTableEvent(tableB, schemaB), null);

        BinaryRecordDataGenerator genA =
                new BinaryRecordDataGenerator(
                        schemaA.getColumnDataTypes().toArray(new DataType[0]));
        BinaryRecordDataGenerator genB =
                new BinaryRecordDataGenerator(
                        schemaB.getColumnDataTypes().toArray(new DataType[0]));

        // TableA: INSERT(id=1,"a") → schema change flush → UPDATE(id=1,"b") [2 batches]
        RecordData rAa = genA.generate(new Object[] {1L, BinaryStringData.fromString("a")});
        icebergWriter.write(DataChangeEvent.insertEvent(tableA, rAa), null);

        AddColumnEvent addColA =
                new AddColumnEvent(
                        tableA,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(addColA);
        icebergWriter.write(addColA, null);

        Schema schemaANew = SchemaUtils.applySchemaChangeEvent(schemaA, addColA);
        BinaryRecordDataGenerator genANew =
                new BinaryRecordDataGenerator(
                        schemaANew.getColumnDataTypes().toArray(new DataType[0]));
        RecordData rAaNew =
                genANew.generate(new Object[] {1L, BinaryStringData.fromString("a"), null});
        RecordData rAb =
                genANew.generate(new Object[] {1L, BinaryStringData.fromString("b"), null});
        icebergWriter.write(DataChangeEvent.updateEvent(tableA, rAaNew, rAb), null);

        // TableB: INSERT(id=2,"x") → UPDATE(id=2,"y") in the same checkpoint, no schema change.
        // Both events land in tableB's single writer (no flush), so dedup is handled internally.
        RecordData rBx = genB.generate(new Object[] {2L, BinaryStringData.fromString("x")});
        RecordData rBy = genB.generate(new Object[] {2L, BinaryStringData.fromString("y")});
        icebergWriter.write(DataChangeEvent.insertEvent(tableB, rBx), null);
        icebergWriter.write(DataChangeEvent.updateEvent(tableB, rBx, rBy), null);

        Collection<WriteResultWrapper> writeResults = icebergWriter.prepareCommit();

        // TableA should produce 2 batches; tableB should produce 1 batch.
        Map<TableId, Long> batchCountByTable =
                writeResults.stream()
                        .collect(
                                Collectors.groupingBy(
                                        WriteResultWrapper::getTableId, Collectors.counting()));
        Assertions.assertThat(batchCountByTable.get(tableA)).isEqualTo(2);
        Assertions.assertThat(batchCountByTable.get(tableB)).isEqualTo(1);

        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions, new HashMap<>());
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);

        // TableA: only final value survives (stale "a" deleted by batch 1's eqDelete).
        List<String> resultA = fetchTableContent(catalog, tableA, null);
        Assertions.assertThat(resultA).containsExactly("1, b, null");

        // TableB: only final value survives (internal position-delete handles dedup within writer).
        List<String> resultB = fetchTableContent(catalog, tableB, null);
        Assertions.assertThat(resultB).containsExactly("2, y");
    }

    /**
     * Verifies that batchIndex stays in sync across subtasks even when a subtask has no writer for
     * the table at schema-change flush time (parallelism > 1 scenario).
     */
    @Test
    public void testBatchIndexInSyncWhenSubtaskHasNoWriterAtSchemaChange() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put(
                "warehouse",
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString());
        catalogOptions.put("cache-enabled", "false");
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();

        // Two subtask writers sharing the same catalog and table.
        IcebergWriter writer0 =
                new IcebergWriter(
                        catalogOptions,
                        0,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergWriter writer1 =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());

        TableId tableId = TableId.parse("test.iceberg_table");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createEvent = new CreateTableEvent(tableId, schema);
        icebergMetadataApplier.applySchemaChange(createEvent);
        writer0.write(createEvent, null);
        writer1.write(createEvent, null);

        BinaryRecordDataGenerator gen =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        // Only subtask 0 has data before the schema change.
        writer0.write(
                DataChangeEvent.insertEvent(
                        tableId, gen.generate(new Object[] {1L, BinaryStringData.fromString("a")})),
                null);
        // Subtask 1 has no writer for the table yet.

        // Both subtasks receive the same SchemaChangeEvent (broadcast).
        AddColumnEvent addColumn =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(addColumn);
        writer0.write(addColumn, null); // has writer → flushes at batchIndex=0; counter → 1
        writer1.write(addColumn, null); // no writer  → counter must still advance to 1

        Schema newSchema = SchemaUtils.applySchemaChangeEvent(schema, addColumn);
        BinaryRecordDataGenerator newGen =
                new BinaryRecordDataGenerator(
                        newSchema.getColumnDataTypes().toArray(new DataType[0]));

        // Subtask 1 writes data after the schema change.
        writer1.write(
                DataChangeEvent.insertEvent(
                        tableId,
                        newGen.generate(new Object[] {2L, BinaryStringData.fromString("b"), null})),
                null);

        Collection<WriteResultWrapper> results0 = writer0.prepareCommit();
        Collection<WriteResultWrapper> results1 = writer1.prepareCommit();

        // subtask 0: one batch at batchIndex=0 (pre-schema-change flush)
        Assertions.assertThat(results0).hasSize(1);
        Assertions.assertThat(results0.iterator().next().getBatchIndex()).isEqualTo(0);

        // subtask 1: must be at batchIndex=1, not 0 — counter advanced at E1 even without a writer
        Assertions.assertThat(results1).hasSize(1);
        Assertions.assertThat(results1.iterator().next().getBatchIndex()).isEqualTo(1);
    }

    /**
     * Verifies no duplicates when parallel subtasks share a table and one subtask has no data
     * before the schema-change flush while the other has an UPDATE that produces an
     * equality-delete.
     */
    @Test
    public void testNoDuplicateWithParallelSubtasksMissingPreSchemaChangeData() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();

        IcebergWriter writer0 =
                new IcebergWriter(
                        catalogOptions,
                        0,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergWriter writer1 =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());

        TableId tableId = TableId.parse("test.iceberg_table");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createEvent = new CreateTableEvent(tableId, initialSchema);
        icebergMetadataApplier.applySchemaChange(createEvent);
        writer0.write(createEvent, null);
        writer1.write(createEvent, null);

        BinaryRecordDataGenerator oldGen =
                new BinaryRecordDataGenerator(
                        initialSchema.getColumnDataTypes().toArray(new DataType[0]));

        // Subtask 1 writes the "old" row before the schema change.
        writer1.write(
                DataChangeEvent.insertEvent(
                        tableId,
                        oldGen.generate(new Object[] {1L, BinaryStringData.fromString("old")})),
                null);
        // Subtask 0 has no data for the table yet.

        // Schema-change E1 is broadcast to both subtasks.
        AddColumnEvent addColumn =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(addColumn);
        writer0.write(addColumn, null); // no writer  → batchIndex must still advance to 1 (fix)
        writer1.write(addColumn, null); // has writer → flushes "old" at batchIndex=0; counter → 1

        // Subtask 0 processes the UPDATE after E1, using the new schema.
        Schema newSchema = SchemaUtils.applySchemaChangeEvent(initialSchema, addColumn);
        BinaryRecordDataGenerator newGen =
                new BinaryRecordDataGenerator(
                        newSchema.getColumnDataTypes().toArray(new DataType[0]));
        RecordData before =
                newGen.generate(new Object[] {1L, BinaryStringData.fromString("old"), null});
        RecordData after =
                newGen.generate(new Object[] {1L, BinaryStringData.fromString("new"), null});
        writer0.write(DataChangeEvent.updateEvent(tableId, before, after), null);

        // Collect and commit all results from both subtasks.
        List<WriteResultWrapper> allResults = new ArrayList<>();
        allResults.addAll(writer0.prepareCommit());
        allResults.addAll(writer1.prepareCommit());

        IcebergCommitter committer = new IcebergCommitter(catalogOptions, new HashMap<>());
        committer.commit(
                allResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList()));

        // Only the updated value must survive; "old" must be deleted by the equality-delete in
        // batch 1 (higher sequence number). Without the fix both rows appear.
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result).containsExactly("1, new, null");
    }

    /**
     * Verifies that wrappers from two subtasks sharing the same batchIndex are merged into exactly
     * one Iceberg snapshot, not two. This directly tests the committer-side grouping fix.
     */
    @Test
    public void testSameBatchIndexFromTwoSubtasksMergedIntoOneSnapshot() throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put(
                "warehouse",
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString());
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();

        IcebergWriter writer0 =
                new IcebergWriter(
                        catalogOptions,
                        0,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergWriter writer1 =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());

        TableId tableId = TableId.parse("test.iceberg_table");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createEvent = new CreateTableEvent(tableId, schema);
        icebergMetadataApplier.applySchemaChange(createEvent);
        writer0.write(createEvent, null);
        writer1.write(createEvent, null);

        BinaryRecordDataGenerator gen =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        // Both subtasks write data with no schema change, so both produce batchIndex=0.
        writer0.write(
                DataChangeEvent.insertEvent(
                        tableId, gen.generate(new Object[] {1L, BinaryStringData.fromString("a")})),
                null);
        writer1.write(
                DataChangeEvent.insertEvent(
                        tableId, gen.generate(new Object[] {2L, BinaryStringData.fromString("b")})),
                null);

        List<WriteResultWrapper> allResults = new ArrayList<>();
        allResults.addAll(writer0.prepareCommit());
        allResults.addAll(writer1.prepareCommit());

        // Both wrappers carry batchIndex=0.
        Assertions.assertThat(allResults).hasSize(2);
        Assertions.assertThat(
                        allResults.stream()
                                .mapToInt(WriteResultWrapper::getBatchIndex)
                                .distinct()
                                .count())
                .isEqualTo(1);

        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));
        long snapshotsBefore = countSnapshots(table);

        IcebergCommitter committer = new IcebergCommitter(catalogOptions, new HashMap<>());
        committer.commit(
                allResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList()));

        table.refresh();
        long snapshotsAfter = countSnapshots(table);

        // Two wrappers with the same batchIndex must produce exactly ONE new snapshot, not two.
        Assertions.assertThat(snapshotsAfter - snapshotsBefore).isEqualTo(1);

        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result).containsExactlyInAnyOrder("1, a", "2, b");
    }

    /**
     * Verifies no duplicates in the most complex parallel scenario: subtask 0 has data only before
     * SC1, subtask 1 has data only between SC1 and SC2, and both have updates after SC2. This
     * exercises all three batchIndex slots across two subtasks simultaneously and confirms that
     * equality-deletes in batch 2 correctly suppress stale data from batches 0 and 1.
     */
    @Test
    public void testNoDuplicateWithMixedDataAcrossSubtasksAndMultipleSchemaChanges()
            throws Exception {
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put(
                "warehouse",
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString());
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        IcebergMetadataApplier icebergMetadataApplier = new IcebergMetadataApplier(catalogOptions);

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();

        IcebergWriter writer0 =
                new IcebergWriter(
                        catalogOptions,
                        0,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());
        IcebergWriter writer1 =
                new IcebergWriter(
                        catalogOptions,
                        1,
                        1,
                        ZoneId.systemDefault(),
                        0,
                        jobId,
                        operatorId,
                        new HashMap<>());

        TableId tableId = TableId.parse("test.iceberg_table");
        Schema schema0 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createEvent = new CreateTableEvent(tableId, schema0);
        icebergMetadataApplier.applySchemaChange(createEvent);
        writer0.write(createEvent, null);
        writer1.write(createEvent, null);

        BinaryRecordDataGenerator gen0 =
                new BinaryRecordDataGenerator(
                        schema0.getColumnDataTypes().toArray(new DataType[0]));

        // Batch 0: only subtask 0 has data before SC1.
        writer0.write(
                DataChangeEvent.insertEvent(
                        tableId,
                        gen0.generate(new Object[] {1L, BinaryStringData.fromString("a")})),
                null);
        // Subtask 1 has no data before SC1.

        // SC1 broadcast to both subtasks.
        AddColumnEvent sc1 =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra1", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(sc1);
        writer0.write(sc1, null); // has writer → flush batchIndex=0; counter → 1
        writer1.write(sc1, null); // no writer  → counter must still advance to 1

        Schema schema1 = SchemaUtils.applySchemaChangeEvent(schema0, sc1);
        BinaryRecordDataGenerator gen1 =
                new BinaryRecordDataGenerator(
                        schema1.getColumnDataTypes().toArray(new DataType[0]));

        // Batch 1: only subtask 1 has data between SC1 and SC2.
        writer1.write(
                DataChangeEvent.insertEvent(
                        tableId,
                        gen1.generate(new Object[] {2L, BinaryStringData.fromString("b"), null})),
                null);
        // Subtask 0 has no data between SC1 and SC2.

        // SC2 broadcast to both subtasks.
        AddColumnEvent sc2 =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra2", DataTypes.STRING(), null, null))));
        icebergMetadataApplier.applySchemaChange(sc2);
        writer0.write(sc2, null); // no writer  → counter must still advance to 2
        writer1.write(sc2, null); // has writer → flush batchIndex=1; counter → 2

        Schema schema2 = SchemaUtils.applySchemaChangeEvent(schema1, sc2);
        BinaryRecordDataGenerator gen2 =
                new BinaryRecordDataGenerator(
                        schema2.getColumnDataTypes().toArray(new DataType[0]));

        // Batch 2: both subtasks update their respective rows after SC2.
        // Subtask 0 updates id=1 "a" → "c"; subtask 1 updates id=2 "b" → "d".
        writer0.write(
                DataChangeEvent.updateEvent(
                        tableId,
                        gen2.generate(
                                new Object[] {1L, BinaryStringData.fromString("a"), null, null}),
                        gen2.generate(
                                new Object[] {1L, BinaryStringData.fromString("c"), null, null})),
                null);
        writer1.write(
                DataChangeEvent.updateEvent(
                        tableId,
                        gen2.generate(
                                new Object[] {2L, BinaryStringData.fromString("b"), null, null}),
                        gen2.generate(
                                new Object[] {2L, BinaryStringData.fromString("d"), null, null})),
                null);

        List<WriteResultWrapper> allResults = new ArrayList<>();
        allResults.addAll(writer0.prepareCommit());
        allResults.addAll(writer1.prepareCommit());

        // Expect 3 batches: {0: sub0}, {1: sub1}, {2: sub0+sub1}
        long distinctBatchIndices =
                allResults.stream().mapToInt(WriteResultWrapper::getBatchIndex).distinct().count();
        Assertions.assertThat(distinctBatchIndices).isEqualTo(3);

        IcebergCommitter committer = new IcebergCommitter(catalogOptions, new HashMap<>());
        committer.commit(
                allResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList()));

        // Only the final values must survive. Equality-deletes in batch 2 (seq N+2) must suppress
        // the stale inserts in batch 0 (seq N) and batch 1 (seq N+1).
        List<String> result = fetchTableContent(catalog, tableId, null);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder("1, c, null, null", "2, d, null, null");
    }

    private static long countSnapshots(Table table) {
        long count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    /** Mock CommitRequestImpl. */
    public static class MockCommitRequestImpl<CommT> extends CommitRequestImpl<CommT> {

        protected MockCommitRequestImpl(CommT committable) {
            super(
                    committable,
                    InternalSinkCommitterMetricGroup.wrap(
                            UnregisteredMetricsGroup.createOperatorMetricGroup()));
        }
    }

    private List<String> fetchTableContent(
            Catalog catalog, TableId tableId, Expression expression) {
        List<String> results = new ArrayList<>();
        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));
        org.apache.iceberg.Schema schema = table.schema();
        IcebergGenerics.ScanBuilder scanbuilder = IcebergGenerics.read(table);
        if (expression != null) {
            scanbuilder = scanbuilder.where(expression);
        }
        CloseableIterable<Record> records = scanbuilder.project(schema).build();
        for (Record record : records) {
            List<String> fieldValues = new ArrayList<>();
            for (Types.NestedField field : schema.columns()) {
                if (field.type() instanceof Types.BinaryType) {
                    java.nio.ByteBuffer byteBuffer =
                            (java.nio.ByteBuffer) record.getField(field.name());
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("[");
                    while (byteBuffer.hasRemaining()) {
                        stringBuilder.append(byteBuffer.get()).append(",");
                    }
                    stringBuilder.append("]");
                    fieldValues.add(stringBuilder.toString());
                } else {
                    String fieldValue = Objects.toString(record.getField(field.name()), "null");
                    fieldValues.add(fieldValue);
                }
            }
            String joinedString = String.join(", ", fieldValues);
            results.add(joinedString);
        }
        return results;
    }
}
