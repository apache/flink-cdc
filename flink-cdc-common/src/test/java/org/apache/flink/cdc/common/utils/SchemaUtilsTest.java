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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A test for the {@link org.apache.flink.cdc.common.utils.SchemaUtils}. */
class SchemaUtilsTest {

    @Test
    void testApplyColumnSchemaChangeEvent() {
        TableId tableId = TableId.parse("default.default.table1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .build();

        // add column in last position
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col3", DataTypes.STRING())));
        AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assertions.assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("col2", DataTypes.STRING())
                                .physicalColumn("col3", DataTypes.STRING())
                                .build());

        // add new column before existed column
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col4", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.BEFORE,
                        "col3"));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assertions.assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("col2", DataTypes.STRING())
                                .physicalColumn("col4", DataTypes.STRING())
                                .physicalColumn("col3", DataTypes.STRING())
                                .build());

        // add new column after existed column
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col5", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.AFTER,
                        "col4"));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assertions.assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("col2", DataTypes.STRING())
                                .physicalColumn("col4", DataTypes.STRING())
                                .physicalColumn("col5", DataTypes.STRING())
                                .physicalColumn("col3", DataTypes.STRING())
                                .build());

        // add column in first position
        addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col0", DataTypes.STRING()),
                        AddColumnEvent.ColumnPosition.FIRST,
                        null));
        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        schema = SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent);
        Assertions.assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("col0", DataTypes.STRING())
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("col2", DataTypes.STRING())
                                .physicalColumn("col4", DataTypes.STRING())
                                .physicalColumn("col5", DataTypes.STRING())
                                .physicalColumn("col3", DataTypes.STRING())
                                .build());

        // drop columns
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(tableId, Arrays.asList("col3", "col5"));
        schema = SchemaUtils.applySchemaChangeEvent(schema, dropColumnEvent);
        Assertions.assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("col0", DataTypes.STRING())
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("col2", DataTypes.STRING())
                                .physicalColumn("col4", DataTypes.STRING())
                                .build());

        // rename columns
        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newCol2");
        nameMapping.put("col4", "newCol4");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, nameMapping);
        schema = SchemaUtils.applySchemaChangeEvent(schema, renameColumnEvent);
        Assertions.assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("col0", DataTypes.STRING())
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("newCol2", DataTypes.STRING())
                                .physicalColumn("newCol4", DataTypes.STRING())
                                .build());

        // alter column types
        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newCol2", DataTypes.VARCHAR(10));
        typeMapping.put("newCol4", DataTypes.VARCHAR(10));
        AlterColumnTypeEvent alterColumnTypeEvent = new AlterColumnTypeEvent(tableId, typeMapping);
        schema = SchemaUtils.applySchemaChangeEvent(schema, alterColumnTypeEvent);
        Assertions.assertThat(schema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("col0", DataTypes.STRING())
                                .physicalColumn("col1", DataTypes.STRING())
                                .physicalColumn("newCol2", DataTypes.VARCHAR(10))
                                .physicalColumn("newCol4", DataTypes.VARCHAR(10))
                                .build());
    }

    @Test
    void testGetNumericPrecision() {
        Assertions.assertThat(SchemaUtils.getNumericPrecision(DataTypes.TINYINT())).isEqualTo(3);
        Assertions.assertThat(SchemaUtils.getNumericPrecision(DataTypes.SMALLINT())).isEqualTo(5);
        Assertions.assertThat(SchemaUtils.getNumericPrecision(DataTypes.INT())).isEqualTo(10);
        Assertions.assertThat(SchemaUtils.getNumericPrecision(DataTypes.BIGINT())).isEqualTo(19);
        Assertions.assertThat(SchemaUtils.getNumericPrecision(DataTypes.DECIMAL(10, 2)))
                .isEqualTo(10);
        Assertions.assertThat(SchemaUtils.getNumericPrecision(DataTypes.DECIMAL(17, 0)))
                .isEqualTo(17);
        Assertions.assertThatThrownBy(() -> SchemaUtils.getNumericPrecision(DataTypes.STRING()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to get precision of non-exact decimal type");
    }

    @Test
    void testInferWiderType() {
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.BINARY(17), DataTypes.BINARY(17)))
                .isEqualTo(DataTypes.BINARY(17));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.VARBINARY(17), DataTypes.VARBINARY(17)))
                .isEqualTo(DataTypes.VARBINARY(17));
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.BYTES(), DataTypes.BYTES()))
                .isEqualTo(DataTypes.BYTES());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.BOOLEAN(), DataTypes.BOOLEAN()))
                .isEqualTo(DataTypes.BOOLEAN());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.INT(), DataTypes.INT()))
                .isEqualTo(DataTypes.INT());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.TINYINT(), DataTypes.TINYINT()))
                .isEqualTo(DataTypes.TINYINT());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.SMALLINT(), DataTypes.SMALLINT()))
                .isEqualTo(DataTypes.SMALLINT());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.BIGINT(), DataTypes.BIGINT()))
                .isEqualTo(DataTypes.BIGINT());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.FLOAT(), DataTypes.FLOAT()))
                .isEqualTo(DataTypes.FLOAT());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.DOUBLE(), DataTypes.DOUBLE()))
                .isEqualTo(DataTypes.DOUBLE());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.CHAR(17), DataTypes.CHAR(17)))
                .isEqualTo(DataTypes.CHAR(17));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.VARCHAR(17), DataTypes.VARCHAR(17)))
                .isEqualTo(DataTypes.VARCHAR(17));
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.STRING(), DataTypes.STRING()))
                .isEqualTo(DataTypes.STRING());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.DECIMAL(17, 7), DataTypes.DECIMAL(17, 7)))
                .isEqualTo(DataTypes.DECIMAL(17, 7));
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.DATE(), DataTypes.DATE()))
                .isEqualTo(DataTypes.DATE());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.TIME(), DataTypes.TIME()))
                .isEqualTo(DataTypes.TIME());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.TIME(6), DataTypes.TIME(6)))
                .isEqualTo(DataTypes.TIME(6));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.TIMESTAMP(), DataTypes.TIMESTAMP()))
                .isEqualTo(DataTypes.TIMESTAMP());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(3)))
                .isEqualTo(DataTypes.TIMESTAMP(3));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_TZ(), DataTypes.TIMESTAMP_TZ()))
                .isEqualTo(DataTypes.TIMESTAMP_TZ());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_TZ(3), DataTypes.TIMESTAMP_TZ(3)))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(3));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_LTZ(), DataTypes.TIMESTAMP_LTZ()))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_LTZ(3), DataTypes.TIMESTAMP_LTZ(3)))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ(3));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.ARRAY(DataTypes.INT()), DataTypes.ARRAY(DataTypes.INT())))
                .isEqualTo(DataTypes.ARRAY(DataTypes.INT()));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                .isEqualTo(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()));

        // Test compatible widening cast
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.INT(), DataTypes.BIGINT()))
                .isEqualTo(DataTypes.BIGINT());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.VARCHAR(17), DataTypes.STRING()))
                .isEqualTo(DataTypes.STRING());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.FLOAT(), DataTypes.DOUBLE()))
                .isEqualTo(DataTypes.DOUBLE());
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.INT(), DataTypes.DECIMAL(4, 0)))
                .isEqualTo(DataTypes.DECIMAL(10, 0));
        Assertions.assertThat(SchemaUtils.inferWiderType(DataTypes.INT(), DataTypes.DECIMAL(10, 5)))
                .isEqualTo(DataTypes.DECIMAL(15, 5));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.BIGINT(), DataTypes.DECIMAL(10, 5)))
                .isEqualTo(DataTypes.DECIMAL(24, 5));
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.DECIMAL(5, 4), DataTypes.DECIMAL(10, 2)))
                .isEqualTo(DataTypes.DECIMAL(12, 4));

        // Test overflow decimal conversions
        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderType(
                                        DataTypes.DECIMAL(5, 5), DataTypes.DECIMAL(38, 0)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Failed to merge DECIMAL(5, 5) NOT NULL and DECIMAL(38, 0) NOT NULL type into DECIMAL. 43 precision digits required, 38 available");

        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderType(
                                        DataTypes.DECIMAL(38, 0), DataTypes.DECIMAL(5, 5)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Failed to merge DECIMAL(38, 0) NOT NULL and DECIMAL(5, 5) NOT NULL type into DECIMAL. 43 precision digits required, 38 available");

        // Test merging with nullability
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.INT().notNull(), DataTypes.INT().notNull()))
                .isEqualTo(DataTypes.INT().notNull());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.INT().nullable(), DataTypes.INT().notNull()))
                .isEqualTo(DataTypes.INT().nullable());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.INT().notNull(), DataTypes.INT().nullable()))
                .isEqualTo(DataTypes.INT().nullable());
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.INT().nullable(), DataTypes.INT().nullable()))
                .isEqualTo(DataTypes.INT().nullable());

        // Test merging temporal types
        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.TIMESTAMP(9), DataTypes.TIMESTAMP(6)))
                .isEqualTo(DataTypes.TIMESTAMP(9));

        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_TZ(3), DataTypes.TIMESTAMP_TZ(7)))
                .isEqualTo(DataTypes.TIMESTAMP_TZ(7));

        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_LTZ(2), DataTypes.TIMESTAMP_LTZ(1)))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ(2));

        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_LTZ(), DataTypes.TIMESTAMP()))
                .isEqualTo(DataTypes.TIMESTAMP(9));

        Assertions.assertThat(
                        SchemaUtils.inferWiderType(DataTypes.TIMESTAMP_TZ(), DataTypes.TIMESTAMP()))
                .isEqualTo(DataTypes.TIMESTAMP(9));

        Assertions.assertThat(
                        SchemaUtils.inferWiderType(
                                DataTypes.TIMESTAMP_LTZ(), DataTypes.TIMESTAMP_TZ()))
                .isEqualTo(DataTypes.TIMESTAMP(9));

        // incompatible type merges test
        Assertions.assertThatThrownBy(
                        () -> SchemaUtils.inferWiderType(DataTypes.INT(), DataTypes.DOUBLE()))
                .isExactlyInstanceOf(IllegalStateException.class);

        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderType(
                                        DataTypes.DECIMAL(17, 0), DataTypes.DOUBLE()))
                .isExactlyInstanceOf(IllegalStateException.class);
        Assertions.assertThatThrownBy(
                        () -> SchemaUtils.inferWiderType(DataTypes.INT(), DataTypes.STRING()))
                .isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    void testInferWiderColumn() {
        // Test normal merges
        Assertions.assertThat(
                        SchemaUtils.inferWiderColumn(
                                Column.physicalColumn("Column1", DataTypes.INT()),
                                Column.physicalColumn("Column1", DataTypes.BIGINT())))
                .isEqualTo(Column.physicalColumn("Column1", DataTypes.BIGINT()));

        Assertions.assertThat(
                        SchemaUtils.inferWiderColumn(
                                Column.physicalColumn("Column2", DataTypes.FLOAT()),
                                Column.physicalColumn("Column2", DataTypes.DOUBLE())))
                .isEqualTo(Column.physicalColumn("Column2", DataTypes.DOUBLE()));

        // Test merging columns with incompatible types
        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderColumn(
                                        Column.physicalColumn("Column3", DataTypes.INT()),
                                        Column.physicalColumn("Column3", DataTypes.STRING())))
                .isExactlyInstanceOf(IllegalStateException.class);

        // Test merging with incompatible names
        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderColumn(
                                        Column.physicalColumn("Column4", DataTypes.INT()),
                                        Column.physicalColumn("AnotherColumn4", DataTypes.INT())))
                .isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    void testInferWiderSchema() {
        // Test normal merges
        Assertions.assertThat(
                        SchemaUtils.inferWiderSchema(
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.INT())
                                        .physicalColumn("Column2", DataTypes.DOUBLE())
                                        .primaryKey("Column1")
                                        .partitionKey("Column2")
                                        .build(),
                                Schema.newBuilder()
                                        .physicalColumn("Column1", DataTypes.BIGINT())
                                        .physicalColumn("Column2", DataTypes.FLOAT())
                                        .primaryKey("Column1")
                                        .partitionKey("Column2")
                                        .build()))
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("Column1", DataTypes.BIGINT())
                                .physicalColumn("Column2", DataTypes.DOUBLE())
                                .primaryKey("Column1")
                                .partitionKey("Column2")
                                .build());

        // Test merging with incompatible types
        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderSchema(
                                        Schema.newBuilder()
                                                .physicalColumn("Column1", DataTypes.INT())
                                                .physicalColumn("Column2", DataTypes.DOUBLE())
                                                .primaryKey("Column1")
                                                .partitionKey("Column2")
                                                .build(),
                                        Schema.newBuilder()
                                                .physicalColumn("Column1", DataTypes.STRING())
                                                .physicalColumn("Column2", DataTypes.STRING())
                                                .primaryKey("Column1")
                                                .partitionKey("Column2")
                                                .build()))
                .isExactlyInstanceOf(IllegalStateException.class);

        // Test merging with incompatible column names
        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderSchema(
                                        Schema.newBuilder()
                                                .physicalColumn("Column1", DataTypes.INT())
                                                .physicalColumn("Column2", DataTypes.DOUBLE())
                                                .primaryKey("Column1")
                                                .partitionKey("Column2")
                                                .build(),
                                        Schema.newBuilder()
                                                .physicalColumn("NotColumn1", DataTypes.INT())
                                                .physicalColumn("NotColumn2", DataTypes.DOUBLE())
                                                .primaryKey("NotColumn1")
                                                .partitionKey("NotColumn2")
                                                .build()))
                .isExactlyInstanceOf(IllegalStateException.class);

        // Test merging with different column counts
        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderSchema(
                                        Schema.newBuilder()
                                                .physicalColumn("Column1", DataTypes.INT())
                                                .physicalColumn("Column2", DataTypes.DOUBLE())
                                                .physicalColumn("Column3", DataTypes.STRING())
                                                .primaryKey("Column1")
                                                .partitionKey("Column2")
                                                .build(),
                                        Schema.newBuilder()
                                                .physicalColumn("NotColumn1", DataTypes.INT())
                                                .physicalColumn("NotColumn2", DataTypes.DOUBLE())
                                                .primaryKey("NotColumn1")
                                                .partitionKey("NotColumn2")
                                                .build()))
                .isExactlyInstanceOf(IllegalStateException.class);

        // Test merging with incompatible schema metadata
        Assertions.assertThatThrownBy(
                        () ->
                                SchemaUtils.inferWiderSchema(
                                        Schema.newBuilder()
                                                .physicalColumn("Column1", DataTypes.INT())
                                                .physicalColumn("Column2", DataTypes.DOUBLE())
                                                .primaryKey("Column1")
                                                .partitionKey("Column2")
                                                .option("Key1", "Value1")
                                                .build(),
                                        Schema.newBuilder()
                                                .physicalColumn("Column1", DataTypes.INT())
                                                .physicalColumn("Column2", DataTypes.DOUBLE())
                                                .primaryKey("Column2")
                                                .partitionKey("Column1")
                                                .option("Key2", "Value2")
                                                .build()))
                .isExactlyInstanceOf(IllegalStateException.class);
    }
}
