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

package org.apache.flink.cdc.connectors.fluss.utils;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussConversions}. */
class FlussConversionsTest {

    // --------------------------------------------------------------------------------------------
    // Tests for toFlussSchema
    // --------------------------------------------------------------------------------------------

    @Test
    void testToFlussSchemaBasic() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        com.alibaba.fluss.metadata.Schema flussSchema = FlussConversions.toFlussSchema(cdcSchema);

        assertThat(flussSchema.getColumnNames()).containsExactly("id", "name");
        assertThat(flussSchema.getPrimaryKeyColumnNames()).containsExactly("id");
    }

    @Test
    void testToFlussSchemaWithoutPrimaryKey() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        com.alibaba.fluss.metadata.Schema flussSchema = FlussConversions.toFlussSchema(cdcSchema);

        assertThat(flussSchema.getColumnNames()).hasSize(2);
        assertThat(flussSchema.getPrimaryKeyColumnNames()).isEmpty();
    }

    @Test
    void testToFlussSchemaTypeConversions() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("bool_col", DataTypes.BOOLEAN())
                        .physicalColumn("tinyint_col", DataTypes.TINYINT())
                        .physicalColumn("smallint_col", DataTypes.SMALLINT())
                        .physicalColumn("int_col", DataTypes.INT())
                        .physicalColumn("bigint_col", DataTypes.BIGINT())
                        .physicalColumn("float_col", DataTypes.FLOAT())
                        .physicalColumn("double_col", DataTypes.DOUBLE())
                        .physicalColumn("decimal_col", DataTypes.DECIMAL(10, 2))
                        .physicalColumn("char_col", DataTypes.CHAR(10))
                        .physicalColumn("varchar_col", DataTypes.VARCHAR(100))
                        .physicalColumn("binary_col", DataTypes.BINARY(16))
                        .physicalColumn("varbinary_col", DataTypes.VARBINARY(100))
                        .physicalColumn("date_col", DataTypes.DATE())
                        .physicalColumn("time_col", DataTypes.TIME())
                        .physicalColumn("timestamp_col", DataTypes.TIMESTAMP(3))
                        .physicalColumn("ltz_col", DataTypes.TIMESTAMP_LTZ(6))
                        .build();

        com.alibaba.fluss.metadata.Schema flussSchema = FlussConversions.toFlussSchema(cdcSchema);

        RowType rowType = flussSchema.getRowType();
        assertThat(rowType.getFieldCount()).isEqualTo(16);

        assertThat(rowType.getTypeAt(0)).isEqualTo(com.alibaba.fluss.types.DataTypes.BOOLEAN());
        assertThat(rowType.getTypeAt(1)).isEqualTo(com.alibaba.fluss.types.DataTypes.TINYINT());
        assertThat(rowType.getTypeAt(2)).isEqualTo(com.alibaba.fluss.types.DataTypes.SMALLINT());
        assertThat(rowType.getTypeAt(3)).isEqualTo(com.alibaba.fluss.types.DataTypes.INT());
        assertThat(rowType.getTypeAt(4)).isEqualTo(com.alibaba.fluss.types.DataTypes.BIGINT());
        assertThat(rowType.getTypeAt(5)).isEqualTo(com.alibaba.fluss.types.DataTypes.FLOAT());
        assertThat(rowType.getTypeAt(6)).isEqualTo(com.alibaba.fluss.types.DataTypes.DOUBLE());
        assertThat(rowType.getTypeAt(7))
                .isEqualTo(com.alibaba.fluss.types.DataTypes.DECIMAL(10, 2));
        assertThat(rowType.getTypeAt(8)).isEqualTo(com.alibaba.fluss.types.DataTypes.CHAR(10));
        // VarChar maps to StringType in Fluss
        assertThat(rowType.getTypeAt(9)).isEqualTo(com.alibaba.fluss.types.DataTypes.STRING());
        assertThat(rowType.getTypeAt(10)).isEqualTo(com.alibaba.fluss.types.DataTypes.BINARY(16));
        // VarBinary maps to BytesType in Fluss
        assertThat(rowType.getTypeAt(11)).isEqualTo(com.alibaba.fluss.types.DataTypes.BYTES());
        assertThat(rowType.getTypeAt(12)).isEqualTo(com.alibaba.fluss.types.DataTypes.DATE());
        assertThat(rowType.getTypeAt(13)).isEqualTo(com.alibaba.fluss.types.DataTypes.TIME());
        assertThat(rowType.getTypeAt(14)).isEqualTo(com.alibaba.fluss.types.DataTypes.TIMESTAMP(3));
        assertThat(rowType.getTypeAt(15))
                .isEqualTo(com.alibaba.fluss.types.DataTypes.TIMESTAMP_LTZ(6));
    }

    @Test
    void testToFlussSchemaTypeNullability() {
        // Verify that nullable flag is correctly propagated from CDC type to Fluss type
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("nullable_col", DataTypes.INT())
                        .physicalColumn("not_null_col", DataTypes.INT().notNull())
                        .build();

        com.alibaba.fluss.metadata.Schema flussSchema = FlussConversions.toFlussSchema(cdcSchema);

        RowType rowType = flussSchema.getRowType();
        assertThat(rowType.getTypeAt(0).isNullable()).isTrue();
        assertThat(rowType.getTypeAt(1).isNullable()).isFalse();
    }

    @Test
    void testToFlussSchemaUnsupportedZonedTimestamp() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn(
                                "ts", new org.apache.flink.cdc.common.types.ZonedTimestampType())
                        .build();

        assertThatThrownBy(() -> FlussConversions.toFlussSchema(cdcSchema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported data type in fluss");
    }

    @Test
    void testToFlussSchemaUnsupportedArrayType() {
        Schema cdcSchema =
                Schema.newBuilder().physicalColumn("arr", DataTypes.ARRAY(DataTypes.INT())).build();

        assertThatThrownBy(() -> FlussConversions.toFlussSchema(cdcSchema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported data type in fluss");
    }

    @Test
    void testToFlussSchemaUnsupportedMapType() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        assertThatThrownBy(() -> FlussConversions.toFlussSchema(cdcSchema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported data type in fluss");
    }

    // --------------------------------------------------------------------------------------------
    // Tests for toFlussTable
    // --------------------------------------------------------------------------------------------

    @Test
    void testToFlussTableBasic() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor descriptor =
                FlussConversions.toFlussTable(cdcSchema, null, null, Collections.emptyMap());

        assertThat(descriptor.getSchema().getColumnNames()).hasSize(2);
        assertThat(descriptor.getSchema().getPrimaryKeyColumnNames()).containsExactly("id");
    }

    @Test
    void testToFlussTableWithBucketKeys() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor descriptor =
                FlussConversions.toFlussTable(
                        cdcSchema, Arrays.asList("id"), 8, Collections.emptyMap());

        assertThat(descriptor.getBucketKeys()).containsExactly("id");
    }

    @Test
    void testToFlussTableWithProperties() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();

        Map<String, String> properties = new HashMap<>();
        properties.put("table.replication.factor", "3");

        TableDescriptor descriptor =
                FlussConversions.toFlussTable(cdcSchema, null, null, properties);

        assertThat(descriptor.getProperties()).containsEntry("table.replication.factor", "3");
    }

    @Test
    void testToFlussTableWithComment() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .comment("my test table")
                        .build();

        TableDescriptor descriptor =
                FlussConversions.toFlussTable(cdcSchema, null, null, Collections.emptyMap());

        assertThat(descriptor.getComment()).hasValue("my test table");
    }

    @Test
    void testToFlussTableWithPartitionKeys() {
        // Partition keys should be propagated to Fluss TableDescriptor
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("dt", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .partitionKey("dt")
                        .build();

        TableDescriptor descriptor =
                FlussConversions.toFlussTable(cdcSchema, null, null, Collections.emptyMap());

        assertThat(descriptor.getPartitionKeys()).containsExactly("dt");
    }

    @Test
    void testToFlussTableDefaultBucketKeysExcludePartitionKeys() {
        // When bucket keys are null, default = (primary keys - partition keys)
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("dt", DataTypes.STRING().notNull())
                        .primaryKey("id", "dt")
                        .partitionKey("dt")
                        .build();

        TableDescriptor descriptor =
                FlussConversions.toFlussTable(cdcSchema, null, null, Collections.emptyMap());

        // "dt" is partition key and should be removed from bucket keys
        assertThat(descriptor.getBucketKeys()).containsExactly("id");
    }

    @Test
    void testToFlussTableDefaultBucketKeysFromPrimaryKeys() {
        Schema cdcSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        // Pass empty bucket keys so it defaults to primary keys
        TableDescriptor descriptor =
                FlussConversions.toFlussTable(
                        cdcSchema, Collections.emptyList(), null, Collections.emptyMap());

        assertThat(descriptor.getBucketKeys()).containsExactly("id");
    }

    // --------------------------------------------------------------------------------------------
    // Tests for sameCdcColumnsIgnoreCommentAndDefaultValue
    // --------------------------------------------------------------------------------------------

    @Test
    void testSameCdcColumnsWithSameColumns() {
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        assertThat(FlussConversions.sameCdcColumnsIgnoreCommentAndDefaultValue(schema1, schema2))
                .isTrue();
    }

    @Test
    void testSameCdcColumnsWithDifferentColumnNames() {
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("age", DataTypes.INT())
                        .build();

        assertThat(FlussConversions.sameCdcColumnsIgnoreCommentAndDefaultValue(schema1, schema2))
                .isFalse();
    }

    @Test
    void testSameCdcColumnsWithDifferentTypes() {
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        assertThat(FlussConversions.sameCdcColumnsIgnoreCommentAndDefaultValue(schema1, schema2))
                .isFalse();
    }

    @Test
    void testSameCdcColumnsWithDifferentColumnCount() {
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        Schema schema2 = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();

        assertThat(FlussConversions.sameCdcColumnsIgnoreCommentAndDefaultValue(schema1, schema2))
                .isFalse();
    }

    @Test
    void testSameCdcColumnsIgnoresComment() {
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), "comment1")
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), "different comment")
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        // Should be true because comments are ignored
        assertThat(FlussConversions.sameCdcColumnsIgnoreCommentAndDefaultValue(schema1, schema2))
                .isTrue();
    }

    @Test
    void testSameCdcColumnsIgnoresDefaultValue() {
        // Default values should be ignored when comparing schemas
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), null, "0")
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), null, "100")
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        assertThat(FlussConversions.sameCdcColumnsIgnoreCommentAndDefaultValue(schema1, schema2))
                .isTrue();
    }
}
