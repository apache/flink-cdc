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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StarRocksUtils}. */
class StarRocksUtilsTest {

    // --------------------------------------------------------------------------------------------
    // Tests for toStarRocksTable
    // --------------------------------------------------------------------------------------------

    @Test
    void testToStarRocksTableBasic() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        TableId tableId = TableId.tableId("my_db", "my_table");
        TableCreateConfig tableCreateConfig = new TableCreateConfig(null, Collections.emptyMap());

        StarRocksTable table = StarRocksUtils.toStarRocksTable(tableId, schema, tableCreateConfig);

        assertThat(table.getDatabaseName()).isEqualTo("my_db");
        assertThat(table.getTableName()).isEqualTo("my_table");
        assertThat(table.getTableType()).isEqualTo(StarRocksTable.TableType.PRIMARY_KEY);
        assertThat(table.getTableKeys()).hasValue(Arrays.asList("id"));
        assertThat(table.getDistributionKeys()).hasValue(Arrays.asList("id"));

        // Verify columns: primary key column should be first
        List<StarRocksColumn> columns = table.getColumns();
        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).getColumnName()).isEqualTo("id");
        assertThat(columns.get(1).getColumnName()).isEqualTo("name");
        assertThat(columns.get(2).getColumnName()).isEqualTo("age");
    }

    @Test
    void testToStarRocksTablePrimaryKeyReordering() {
        // Define schema where primary key columns are NOT at the front
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .physicalColumn("age", DataTypes.INT())
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .build();

        TableId tableId = TableId.tableId("db", "table");
        TableCreateConfig config = new TableCreateConfig(null, Collections.emptyMap());

        StarRocksTable table = StarRocksUtils.toStarRocksTable(tableId, schema, config);

        // Primary key "id" should be reordered to the front
        List<StarRocksColumn> columns = table.getColumns();
        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).getColumnName()).isEqualTo("id");
        assertThat(columns.get(1).getColumnName()).isEqualTo("name");
        assertThat(columns.get(2).getColumnName()).isEqualTo("age");
    }

    @Test
    void testToStarRocksTableCompositePrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("data", DataTypes.VARCHAR(200))
                        .physicalColumn("id1", DataTypes.INT().notNull())
                        .physicalColumn("id2", DataTypes.BIGINT().notNull())
                        .primaryKey("id1", "id2")
                        .build();

        TableId tableId = TableId.tableId("db", "table");
        TableCreateConfig config = new TableCreateConfig(null, Collections.emptyMap());

        StarRocksTable table = StarRocksUtils.toStarRocksTable(tableId, schema, config);

        // Both primary key columns should be at the front, in the order of primaryKeys definition
        List<StarRocksColumn> columns = table.getColumns();
        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).getColumnName()).isEqualTo("id1");
        assertThat(columns.get(1).getColumnName()).isEqualTo("id2");
        assertThat(columns.get(2).getColumnName()).isEqualTo("data");

        assertThat(table.getTableKeys()).hasValue(Arrays.asList("id1", "id2"));
        assertThat(table.getDistributionKeys()).hasValue(Arrays.asList("id1", "id2"));
    }

    @Test
    void testToStarRocksTableNoPrimaryKeyThrowsException() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .build();

        TableId tableId = TableId.tableId("db", "table");
        TableCreateConfig config = new TableCreateConfig(null, Collections.emptyMap());

        assertThatThrownBy(() -> StarRocksUtils.toStarRocksTable(tableId, schema, config))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("has no primary keys");
    }

    @Test
    void testToStarRocksTableWithNumBuckets() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .build();

        TableId tableId = TableId.tableId("db", "table");
        TableCreateConfig config = new TableCreateConfig(10, Collections.emptyMap());

        StarRocksTable table = StarRocksUtils.toStarRocksTable(tableId, schema, config);
        assertThat(table.getNumBuckets()).hasValue(10);
    }

    @Test
    void testToStarRocksTableWithProperties() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .build();

        TableId tableId = TableId.tableId("db", "table");
        Map<String, String> properties = new HashMap<>();
        properties.put("replication_num", "3");
        TableCreateConfig config = new TableCreateConfig(null, properties);

        StarRocksTable table = StarRocksUtils.toStarRocksTable(tableId, schema, config);
        assertThat(table.getProperties()).containsEntry("replication_num", "3");
    }

    @Test
    void testToStarRocksTableWithComment() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .comment("test table comment")
                        .build();

        TableId tableId = TableId.tableId("db", "table");
        TableCreateConfig config = new TableCreateConfig(null, Collections.emptyMap());

        StarRocksTable table = StarRocksUtils.toStarRocksTable(tableId, schema, config);
        assertThat(table.getComment()).hasValue("test table comment");
    }

    // --------------------------------------------------------------------------------------------
    // Tests for convertInvalidTimestampDefaultValue
    // --------------------------------------------------------------------------------------------

    @Test
    void testConvertInvalidTimestampDefaultValueWithNull() {
        assertThat(StarRocksUtils.convertInvalidTimestampDefaultValue(null, new TimestampType()))
                .isNull();
    }

    @Test
    void testConvertInvalidTimestampDefaultValueWithValidValue() {
        String validDefault = "2024-01-01 12:00:00";
        assertThat(
                        StarRocksUtils.convertInvalidTimestampDefaultValue(
                                validDefault, new TimestampType()))
                .isEqualTo(validDefault);
    }

    @Test
    void testConvertInvalidTimestampDefaultValueWithInvalidTimestamp() {
        assertThat(
                        StarRocksUtils.convertInvalidTimestampDefaultValue(
                                "0000-00-00 00:00:00", new TimestampType()))
                .isEqualTo(StarRocksUtils.DEFAULT_DATETIME);
    }

    @Test
    void testConvertInvalidTimestampDefaultValueWithLocalZonedTimestamp() {
        assertThat(
                        StarRocksUtils.convertInvalidTimestampDefaultValue(
                                "0000-00-00 00:00:00", new LocalZonedTimestampType()))
                .isEqualTo(StarRocksUtils.DEFAULT_DATETIME);
    }

    @Test
    void testConvertInvalidTimestampDefaultValueWithZonedTimestamp() {
        // ZonedTimestampType is also a timestamp type, should convert invalid values
        assertThat(
                        StarRocksUtils.convertInvalidTimestampDefaultValue(
                                "0000-00-00 00:00:00", new ZonedTimestampType()))
                .isEqualTo(StarRocksUtils.DEFAULT_DATETIME);
    }

    @Test
    void testConvertInvalidTimestampDefaultValueWithNonTimestampType() {
        // For non-timestamp types, the invalid datetime string should be returned as-is
        assertThat(
                        StarRocksUtils.convertInvalidTimestampDefaultValue(
                                "0000-00-00 00:00:00", DataTypes.VARCHAR(100)))
                .isEqualTo("0000-00-00 00:00:00");
    }

    // --------------------------------------------------------------------------------------------
    // Tests for toStarRocksDataType
    // --------------------------------------------------------------------------------------------

    @Test
    void testToStarRocksDataTypeNullableFlag() {
        // nullable type should set isNullable=true
        StarRocksColumn.Builder nullableBuilder =
                new StarRocksColumn.Builder().setColumnName("col").setOrdinalPosition(0);
        StarRocksUtils.toStarRocksDataType(new IntType(), false, nullableBuilder);
        StarRocksColumn nullableCol = nullableBuilder.build();
        assertThat(nullableCol.getDataType()).isEqualTo(StarRocksUtils.INT);
        assertThat(nullableCol.isNullable()).isTrue();

        // not-null type should set isNullable=false
        StarRocksColumn.Builder notNullBuilder =
                new StarRocksColumn.Builder().setColumnName("col").setOrdinalPosition(0);
        StarRocksUtils.toStarRocksDataType(new IntType(false), false, notNullBuilder);
        StarRocksColumn notNullCol = notNullBuilder.build();
        assertThat(notNullCol.getDataType()).isEqualTo(StarRocksUtils.INT);
        assertThat(notNullCol.isNullable()).isFalse();
    }

    @Test
    void testToStarRocksDataTypeAllBasicTypes() {
        // Verify data type mapping for all basic CDC types
        assertStarRocksDataType(new BooleanType(), StarRocksUtils.BOOLEAN);
        assertStarRocksDataType(new TinyIntType(), StarRocksUtils.TINYINT);
        assertStarRocksDataType(new SmallIntType(), StarRocksUtils.SMALLINT);
        assertStarRocksDataType(new IntType(), StarRocksUtils.INT);
        assertStarRocksDataType(new BigIntType(), StarRocksUtils.BIGINT);
        assertStarRocksDataType(new FloatType(), StarRocksUtils.FLOAT);
        assertStarRocksDataType(new DoubleType(), StarRocksUtils.DOUBLE);
        assertStarRocksDataType(new DateType(), StarRocksUtils.DATE);
        assertStarRocksDataType(new TimestampType(), StarRocksUtils.DATETIME);
        assertStarRocksDataType(new LocalZonedTimestampType(), StarRocksUtils.DATETIME);
    }

    @Test
    void testToStarRocksDataTypeUnsupported() {
        StarRocksColumn.Builder builder =
                new StarRocksColumn.Builder().setColumnName("col").setOrdinalPosition(0);
        assertThatThrownBy(
                        () -> StarRocksUtils.toStarRocksDataType(DataTypes.BYTES(), false, builder))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported CDC data type");
    }

    private void assertStarRocksDataType(DataType cdcType, String expectedStarRocksType) {
        StarRocksColumn.Builder builder =
                new StarRocksColumn.Builder().setColumnName("col").setOrdinalPosition(0);
        StarRocksUtils.toStarRocksDataType(cdcType, false, builder);
        assertThat(builder.build().getDataType()).isEqualTo(expectedStarRocksType);
    }

    // --------------------------------------------------------------------------------------------
    // Tests for createFieldGetter
    // --------------------------------------------------------------------------------------------

    @Test
    void testCreateFieldGetterBoolean() {
        RecordData.FieldGetter getter =
                StarRocksUtils.createFieldGetter(new BooleanType(), 0, ZoneId.of("UTC"));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(new DataType[] {new BooleanType()});
        BinaryRecordData record = generator.generate(new Object[] {true});
        assertThat(getter.getFieldOrNull(record)).isEqualTo(true);
    }

    @Test
    void testCreateFieldGetterInt() {
        RecordData.FieldGetter getter =
                StarRocksUtils.createFieldGetter(new IntType(), 0, ZoneId.of("UTC"));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(new DataType[] {new IntType()});
        BinaryRecordData record = generator.generate(new Object[] {42});
        assertThat(getter.getFieldOrNull(record)).isEqualTo(42);
    }

    @Test
    void testCreateFieldGetterVarChar() {
        RecordData.FieldGetter getter =
                StarRocksUtils.createFieldGetter(new VarCharType(100), 0, ZoneId.of("UTC"));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(new DataType[] {new VarCharType(100)});
        BinaryRecordData record =
                generator.generate(new Object[] {BinaryStringData.fromString("hello")});
        assertThat(getter.getFieldOrNull(record)).isEqualTo("hello");
    }

    @Test
    void testCreateFieldGetterDate() {
        // Date field getter should format output as "yyyy-MM-dd"
        RecordData.FieldGetter getter =
                StarRocksUtils.createFieldGetter(new DateType(), 0, ZoneId.of("UTC"));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(new DataType[] {new DateType()});
        BinaryRecordData record =
                generator.generate(
                        new Object[] {DateData.fromLocalDate(LocalDate.of(2024, 1, 15))});
        assertThat(getter.getFieldOrNull(record)).isEqualTo("2024-01-15");
    }

    @Test
    void testCreateFieldGetterTimestamp() {
        // Timestamp field getter should format output as "yyyy-MM-dd HH:mm:ss"
        RecordData.FieldGetter getter =
                StarRocksUtils.createFieldGetter(new TimestampType(3), 0, ZoneId.of("UTC"));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(new DataType[] {new TimestampType(3)});
        BinaryRecordData record =
                generator.generate(
                        new Object[] {
                            TimestampData.fromLocalDateTime(
                                    LocalDateTime.of(2024, 1, 15, 10, 30, 0))
                        });
        assertThat(getter.getFieldOrNull(record)).isEqualTo("2024-01-15 10:30:00");
    }

    @Test
    void testCreateFieldGetterLocalZonedTimestamp() {
        // LocalZonedTimestamp field getter should convert to the specified zone
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        RecordData.FieldGetter getter =
                StarRocksUtils.createFieldGetter(new LocalZonedTimestampType(6), 0, zoneId);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(new DataType[] {new LocalZonedTimestampType(6)});
        BinaryRecordData record =
                generator.generate(
                        new Object[] {
                            LocalZonedTimestampData.fromInstant(
                                    LocalDateTime.of(2024, 1, 15, 10, 30, 0)
                                            .toInstant(ZoneOffset.UTC))
                        });
        // UTC 10:30 -> Asia/Shanghai 18:30
        assertThat(getter.getFieldOrNull(record)).isEqualTo("2024-01-15 18:30:00");
    }

    @Test
    void testCreateFieldGetterNullable() {
        // For nullable types, null values should return null
        RecordData.FieldGetter getter =
                StarRocksUtils.createFieldGetter(DataTypes.INT(), 0, ZoneId.of("UTC"));

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(new DataType[] {DataTypes.INT()});
        BinaryRecordData record = generator.generate(new Object[] {null});
        assertThat(getter.getFieldOrNull(record)).isNull();
    }

    @Test
    void testCreateFieldGetterUnsupportedType() {
        assertThatThrownBy(
                        () ->
                                StarRocksUtils.createFieldGetter(
                                        DataTypes.BYTES(), 0, ZoneId.of("UTC")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Don't support data type");
    }
}
