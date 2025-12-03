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

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryMapData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonMetadataApplier;
import org.apache.flink.cdc.runtime.serializer.data.MapDataSerializer;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.NestedRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowKind;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Tests for {@link PaimonWriterHelper}. */
class PaimonWriterHelperTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    void testConvertEventToGenericRowOfAllDataTypes() {
        RowType rowType =
                RowType.of(
                        DataTypes.BOOLEAN(),
                        DataTypes.BINARY(3),
                        DataTypes.VARBINARY(10),
                        DataTypes.BYTES(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(6, 3),
                        DataTypes.CHAR(5),
                        DataTypes.VARCHAR(10),
                        DataTypes.STRING(),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.STRING());
        Object[] testData =
                new Object[] {
                    true,
                    new byte[] {1, 2},
                    new byte[] {3, 4},
                    new byte[] {5, 6, 7},
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    5.1f,
                    6.2,
                    DecimalData.fromBigDecimal(new BigDecimal("7.123"), 6, 3),
                    BinaryStringData.fromString("test1"),
                    BinaryStringData.fromString("test2"),
                    BinaryStringData.fromString("test3"),
                    DateData.fromEpochDay(100),
                    TimeData.fromNanoOfDay(200_000_000L),
                    TimeData.fromNanoOfDay(300_000_000L),
                    TimestampData.fromTimestamp(
                            java.sql.Timestamp.valueOf("2023-01-01 00:00:00.000")),
                    TimestampData.fromTimestamp(java.sql.Timestamp.valueOf("2023-01-01 00:00:00")),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2023-01-01T00:00:00.000Z")),
                    LocalZonedTimestampData.fromInstant(Instant.parse("2023-01-01T00:00:00.000Z")),
                    null
                };
        BinaryRecordData recordData = new BinaryRecordDataGenerator(rowType).generate(testData);
        Schema schema = Schema.newBuilder().fromRowDataType(rowType).build();
        List<RecordData.FieldGetter> fieldGetters =
                PaimonWriterHelper.createFieldGetters(schema, ZoneId.of("UTC+8"));
        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(TableId.parse("database.table"), recordData);
        GenericRow genericRow =
                PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertThat(genericRow)
                .isEqualTo(
                        GenericRow.ofKind(
                                RowKind.INSERT,
                                true,
                                new byte[] {1, 2},
                                new byte[] {3, 4},
                                new byte[] {5, 6, 7},
                                (byte) 1,
                                (short) 2,
                                3,
                                4L,
                                5.1f,
                                6.2,
                                Decimal.fromBigDecimal(new BigDecimal("7.123"), 6, 3),
                                BinaryString.fromString("test1"),
                                BinaryString.fromString("test2"),
                                BinaryString.fromString("test3"),
                                100,
                                200,
                                300,
                                Timestamp.fromSQLTimestamp(
                                        java.sql.Timestamp.valueOf("2023-01-01 00:00:00.000")),
                                Timestamp.fromSQLTimestamp(
                                        java.sql.Timestamp.valueOf("2023-01-01 00:00:00")),
                                Timestamp.fromInstant(Instant.parse("2023-01-01T00:00:00.000Z")),
                                Timestamp.fromInstant(Instant.parse("2023-01-01T00:00:00.000Z")),
                                null));
    }

    @Test
    void testConvertEventToGenericRowOfDataChangeTypes() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .build();
        List<RecordData.FieldGetter> fieldGetters =
                PaimonWriterHelper.createFieldGetters(schema, ZoneId.systemDefault());
        TableId tableId = TableId.parse("database.table");
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        BinaryRecordData recordData =
                generator.generate(
                        new Object[] {
                            BinaryStringData.fromString("1"), BinaryStringData.fromString("1")
                        });

        DataChangeEvent dataChangeEvent = DataChangeEvent.insertEvent(tableId, recordData);
        GenericRow genericRow =
                PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertThat(genericRow.getRowKind()).isEqualTo(RowKind.INSERT);

        dataChangeEvent = DataChangeEvent.deleteEvent(tableId, recordData);
        genericRow = PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertThat(genericRow.getRowKind()).isEqualTo(RowKind.DELETE);

        dataChangeEvent = DataChangeEvent.updateEvent(tableId, recordData, recordData);
        genericRow = PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertThat(genericRow.getRowKind()).isEqualTo(RowKind.INSERT);

        dataChangeEvent = DataChangeEvent.replaceEvent(tableId, recordData, null);
        genericRow = PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);
        Assertions.assertThat(genericRow.getRowKind()).isEqualTo(RowKind.INSERT);
    }

    @Test
    void testConvertEventToGenericRowWithNestedRow() {
        // Define the inner row type with an integer and a map
        RowType innerRowType =
                RowType.of(DataTypes.INT(), DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));

        // Define the outer row type with a nested row
        RowType rowType = RowType.of(DataTypes.ROW(innerRowType));

        // Create a map serializer and a map data instance
        MapDataSerializer mapDataserializer =
                new MapDataSerializer(DataTypes.STRING(), DataTypes.STRING());
        Map<BinaryStringData, BinaryStringData> map = new HashMap<>();
        map.put(BinaryStringData.fromString("key1"), BinaryStringData.fromString("value1"));
        map.put(BinaryStringData.fromString("key2"), BinaryStringData.fromString("value2"));
        GenericMapData genMapData = new GenericMapData(map);
        BinaryMapData binMap = mapDataserializer.toBinaryMap(genMapData);

        // Create inner row data
        Object[] innerRowData =
                new Object[] {
                    42, // Integer field
                    binMap // Map field
                };

        // Generate inner BinaryRecordData
        BinaryRecordData innerRecordData =
                new BinaryRecordDataGenerator(innerRowType).generate(innerRowData);

        // Create outer row data containing the inner record data
        Object[] testData = new Object[] {innerRecordData};

        // Generate outer BinaryRecordData
        BinaryRecordData recordData = new BinaryRecordDataGenerator(rowType).generate(testData);

        // Create schema and field getters
        Schema schema = Schema.newBuilder().fromRowDataType(rowType).build();
        List<RecordData.FieldGetter> fieldGetters =
                PaimonWriterHelper.createFieldGetters(schema, ZoneId.of("UTC+8"));

        // Create a data change event
        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(TableId.parse("database.table"), recordData);

        // Convert the event to GenericRow
        GenericRow genericRow =
                PaimonWriterHelper.convertEventToGenericRow(dataChangeEvent, fieldGetters);

        // Verify the nested row field
        NestedRow innerRow = (NestedRow) genericRow.getField(0);
        NestedRow nestedRow = (NestedRow) innerRow.getRow(0, 2);
        int actual = nestedRow.getRow(0, 2).getInt(0); // 42
        Assertions.assertThat(actual).isEqualTo(42);
        Map<BinaryString, BinaryString> expectedMap = new HashMap<>();
        expectedMap.put(BinaryString.fromString("key1"), BinaryString.fromString("value1"));
        expectedMap.put(BinaryString.fromString("key2"), BinaryString.fromString("value2"));
        InternalMap internalMap = nestedRow.getMap(1);
        Map<BinaryString, BinaryString> extractedMap = extractMap(internalMap);

        Assertions.assertThat(extractedMap).containsAllEntriesOf(expectedMap);
    }

    @Test
    void testSameColumnsWithOutCommentAndDefaultValue() {
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING(), "col1", "default")
                        .physicalColumn("col2", DataTypes.STRING(), "col2", "default")
                        .physicalColumn("col3", DataTypes.STRING(), "col3", "default")
                        .primaryKey("col1")
                        .partitionKey("col2")
                        .comment("schema1")
                        .option("key", "value")
                        .build();
        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build();
        Assertions.assertThat(
                        PaimonWriterHelper.sameColumnsIgnoreCommentAndDefaultValue(
                                schema1, schema2))
                .isTrue();

        // Not null.
        schema1 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING(), "col1", "default")
                        .physicalColumn("col2", DataTypes.STRING(), "col2", "default")
                        .physicalColumn("col3", DataTypes.STRING().notNull(), "col3", "default")
                        .primaryKey("col1")
                        .partitionKey("col2")
                        .comment("schema1")
                        .option("key", "value")
                        .build();
        schema2 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build();
        Assertions.assertThat(
                        PaimonWriterHelper.sameColumnsIgnoreCommentAndDefaultValue(
                                schema1, schema2))
                .isFalse();

        schema1 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .physicalColumn("col4", DataTypes.STRING())
                        .build();
        schema2 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build();
        Assertions.assertThat(
                        PaimonWriterHelper.sameColumnsIgnoreCommentAndDefaultValue(
                                schema1, schema2))
                .isFalse();

        // Different order.
        schema1 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .build();
        schema2 =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .build();
        Assertions.assertThat(
                        PaimonWriterHelper.sameColumnsIgnoreCommentAndDefaultValue(
                                schema1, schema2))
                .isFalse();
    }

    @Test
    void testDeduceSchemaForPaimonTable() throws Catalog.TableNotExistException {
        Options catalogOptions = new Options();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        Schema allTypeSchema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING().notNull())
                        .physicalColumn("boolean", DataTypes.BOOLEAN())
                        .physicalColumn("binary", DataTypes.BINARY(3))
                        .physicalColumn("varbinary", DataTypes.VARBINARY(10))
                        .physicalColumn("bytes", DataTypes.BYTES())
                        .physicalColumn("tinyint", DataTypes.TINYINT())
                        .physicalColumn("smallint", DataTypes.SMALLINT())
                        .physicalColumn("int", DataTypes.INT())
                        .physicalColumn("float", DataTypes.FLOAT())
                        .physicalColumn("double", DataTypes.DOUBLE())
                        .physicalColumn("decimal", DataTypes.DECIMAL(6, 3))
                        .physicalColumn("char", DataTypes.CHAR(5))
                        .physicalColumn("varchar", DataTypes.VARCHAR(10))
                        .physicalColumn("string", DataTypes.STRING())
                        .physicalColumn("date", DataTypes.DATE())
                        .physicalColumn("time", DataTypes.TIME())
                        .physicalColumn("time_with_precision", DataTypes.TIME(6))
                        .physicalColumn("timestamp", DataTypes.TIMESTAMP())
                        .physicalColumn("timestamp_with_precision", DataTypes.TIMESTAMP(3))
                        .physicalColumn("timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                        .physicalColumn("timestamp_ltz_with_precision", DataTypes.TIMESTAMP_LTZ(3))
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TableId.parse("test.table1"), allTypeSchema);
        metadataApplier.applySchemaChange(createTableEvent);
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        Table table = catalog.getTable(Identifier.fromString("test.table1"));
        Schema deducedSchema = PaimonWriterHelper.deduceSchemaForPaimonTable(table);
        Assertions.assertThat(
                        PaimonWriterHelper.sameColumnsIgnoreCommentAndDefaultValue(
                                deducedSchema, allTypeSchema))
                .isTrue();
    }

    private static Map<BinaryString, BinaryString> extractMap(InternalMap internalMap) {
        Map<BinaryString, BinaryString> resultMap = new HashMap<>();
        for (int i = 0; i < internalMap.size(); i++) {
            BinaryString key = internalMap.keyArray().getString(i);
            BinaryString value = internalMap.valueArray().getString(i);
            resultMap.put(key, value);
        }
        return resultMap;
    }
}
