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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.connectors.fluss.utils.FlussConversions.toFlussSchema;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests compatibility checks for externally managed Fluss schemas. */
class FlussSchemaValidatorTest {
    private static final TablePath TABLE = TablePath.of("db", "tbl");

    @Test
    void acceptsReorderedSupersetAndNormalizedPrimaryKey() {
        Schema source =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("value", DataTypes.VARCHAR(20).notNull())
                        .physicalColumn(
                                "nested", DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())))
                        .primaryKey("id")
                        .build();
        Schema target =
                Schema.newBuilder()
                        .physicalColumn("value", DataTypes.STRING())
                        .physicalColumn("extra", DataTypes.STRING())
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn(
                                "nested", DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())))
                        .primaryKey("id")
                        .build();

        FlussSchemaValidator.validateSchema(toFlussSchema(source), toFlussSchema(target), TABLE);
    }

    @Test
    void rejectsMissingWrongTypeAndUnsafeNullability() {
        Schema source = Schema.newBuilder().physicalColumn("value", DataTypes.INT()).build();
        assertIncompatible(
                source,
                Schema.newBuilder().physicalColumn("Value", DataTypes.INT()).build(),
                "missing");
        assertIncompatible(
                source,
                Schema.newBuilder().physicalColumn("value", DataTypes.BIGINT()).build(),
                "cannot contain");
        assertIncompatible(
                source,
                Schema.newBuilder().physicalColumn("value", DataTypes.INT().notNull()).build(),
                "cannot contain");
    }

    @Test
    void rejectsTargetOnlyNotNullColumn() {
        Schema source = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        Schema target =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("extra", DataTypes.STRING().notNull())
                        .build();
        assertIncompatible(source, target, "Target-only column extra");
    }

    private static void assertIncompatible(Schema source, Schema target, String message) {
        assertThatThrownBy(
                        () ->
                                FlussSchemaValidator.validateSchema(
                                        toFlussSchema(source), toFlussSchema(target), TABLE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }
}
