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

package org.apache.flink.cdc.connectors.lancedb.utils;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link LanceDbTypeUtils}. */
class LanceDbTypeUtilsTest {

    @Test
    void testMapsPrimitiveTypes() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("c_bool", DataTypes.BOOLEAN())
                        .physicalColumn("c_int", DataTypes.INT())
                        .physicalColumn("c_bigint", DataTypes.BIGINT())
                        .physicalColumn("c_float", DataTypes.FLOAT())
                        .physicalColumn("c_double", DataTypes.DOUBLE())
                        .physicalColumn("c_string", DataTypes.STRING())
                        .physicalColumn("c_bytes", DataTypes.BYTES())
                        .physicalColumn("c_decimal", DataTypes.DECIMAL(10, 2))
                        .physicalColumn("c_date", DataTypes.DATE())
                        .physicalColumn("c_time", DataTypes.TIME())
                        .physicalColumn("c_ts", DataTypes.TIMESTAMP())
                        .build();

        org.apache.arrow.vector.types.pojo.Schema arrowSchema =
                LanceDbTypeUtils.toArrowSchema(schema, true);

        Assertions.assertThat(arrowSchema.findField("c_bool").getType())
                .isInstanceOf(ArrowType.Bool.class);
        Assertions.assertThat(arrowSchema.findField("c_int").isNullable()).isTrue();
        Assertions.assertThat(arrowSchema.findField("c_bigint").getType())
                .isEqualTo(new ArrowType.Int(64, true));
        Assertions.assertThat(arrowSchema.findField("c_string").getType())
                .isInstanceOf(ArrowType.Utf8.class);
        Assertions.assertThat(arrowSchema.findField("_cdc_op")).isNotNull();
    }

    @Test
    void testRejectsRowType() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn(
                                "nested", DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())))
                        .build();

        Assertions.assertThatThrownBy(() -> LanceDbTypeUtils.toArrowSchema(schema, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("ROW");
    }

    @Test
    void testPreservesNotNullFields() {
        Schema schema = Schema.newBuilder().physicalColumn("id", DataTypes.INT().notNull()).build();

        org.apache.arrow.vector.types.pojo.Schema arrowSchema =
                LanceDbTypeUtils.toArrowSchema(schema, false);

        Assertions.assertThat(arrowSchema.findField("id").isNullable()).isFalse();
    }

    @Test
    void testRejectsUnsupportedArrayElementType() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("prices", DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)))
                        .build();

        Assertions.assertThatThrownBy(() -> LanceDbTypeUtils.toArrowSchema(schema, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("ARRAY elements");
    }

    @Test
    void testRejectsReservedCdcMetadataColumnInAppendWithMetadataMode() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("_cdc_op", DataTypes.STRING())
                        .physicalColumn("payload", DataTypes.STRING())
                        .build();

        Assertions.assertThatThrownBy(() -> LanceDbTypeUtils.toArrowSchema(schema, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reserved LanceDB CDC metadata column _cdc_op");
    }

    @Test
    void testValidateCompatibleRejectsNullabilityMismatch() {
        Field expected = LanceDbTypeUtils.toArrowField("id", DataTypes.INT().notNull(), false);
        Field actual = LanceDbTypeUtils.toArrowField("id", DataTypes.INT(), true);

        Assertions.assertThatThrownBy(
                        () ->
                                LanceDbTypeUtils.validateCompatibleField(
                                        "/tmp/products.lance", expected, actual, "id"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("field mismatch at id");
    }

    @Test
    void testValidateCompatibleRejectsArrayElementMismatch() {
        Field expected =
                LanceDbTypeUtils.toArrowField("tags", DataTypes.ARRAY(DataTypes.STRING()), true);
        Field actual =
                LanceDbTypeUtils.toArrowField("tags", DataTypes.ARRAY(DataTypes.INT()), true);

        Assertions.assertThatThrownBy(
                        () ->
                                LanceDbTypeUtils.validateCompatibleField(
                                        "/tmp/products.lance", expected, actual, "tags"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tags.element");
    }
}
