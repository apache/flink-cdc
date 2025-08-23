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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
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
        IcebergWriter icebergWriter =
                new IcebergWriter(catalogOptions, 1, 1, ZoneId.systemDefault());
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
        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions);
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);
        List<String> result = fetchTableContent(catalog, tableId);
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
        icebergCommitter.commit(collection);
        result = fetchTableContent(catalog, tableId);
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
        IcebergWriter icebergWriter = new IcebergWriter(catalogOptions, 1, 1, pipelineZoneId);
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
                                .physicalColumn("smallint", DataTypes.SMALLINT())
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
                            (short) 2,
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
        IcebergCommitter icebergCommitter = new IcebergCommitter(catalogOptions);
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writeResults.stream().map(MockCommitRequestImpl::new).collect(Collectors.toList());
        icebergCommitter.commit(collection);
        List<String> result = fetchTableContent(catalog, tableId);
        Assertions.assertThat(result)
                .containsExactlyInAnyOrder(
                        "char, varchar, string, false, [1,2,3,4,5,], [1,2,3,4,5,6,7,8,9,10,], 0.00, 1, 2, 12345, 12345, 123.456, 123456.789, 00:00:12.345, 2003-10-20, 1970-01-01T00:00, 1970-01-01T00:00Z, 1970-01-01T00:00Z");
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

    private List<String> fetchTableContent(Catalog catalog, TableId tableId) {
        List<String> results = new ArrayList<>();
        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));
        org.apache.iceberg.Schema schema = table.schema();
        CloseableIterable<Record> records = IcebergGenerics.read(table).project(schema).build();
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
