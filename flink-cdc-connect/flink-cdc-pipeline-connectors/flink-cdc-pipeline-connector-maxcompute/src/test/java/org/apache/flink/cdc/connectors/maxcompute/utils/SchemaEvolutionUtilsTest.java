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

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.maxcompute.EmulatorTestBase;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * e2e test of SchemaEvolutionUtils, Note that the Emulator only supports uppercase input (However,
 * MaxCompute can correctly distinguish between uppercase and lowercase).
 *
 * <p>Since the emulator does not support alter column type, the test cases here are mainly for
 * testing other schema evolution logic.
 */
class SchemaEvolutionUtilsTest extends EmulatorTestBase {

    private static final String TEST_TABLE = "SCHEMA_EVOLUTION_TEST_TABLE";

    @BeforeEach
    void testCreateTable() {
        try {
            SchemaEvolutionUtils.createTable(
                    testOptions,
                    TableId.tableId(TEST_TABLE),
                    Schema.newBuilder()
                            .physicalColumn("PK", DataTypes.BIGINT())
                            .physicalColumn("ID1", DataTypes.BIGINT())
                            .physicalColumn("ID2", DataTypes.BIGINT())
                            .primaryKey("PK")
                            .build());
            Assertions.assertEquals(
                    ImmutableList.of("PK"), odps.tables().get(TEST_TABLE).getPrimaryKey());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @AfterEach
    void deleteTable() throws OdpsException {
        odps.tables().delete(TEST_TABLE, true);
    }

    @Test
    void testAddColumn() throws OdpsException {
        try {
            SchemaEvolutionUtils.addColumns(
                    testOptions,
                    TableId.tableId(TEST_TABLE),
                    ImmutableList.of(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("ID3", DataTypes.BIGINT())),
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("NAME", DataTypes.STRING()))));
            TableSchema schema = odps.tables().get(TEST_TABLE).getSchema();

            Assertions.assertEquals(5, schema.getColumns().size());
            Assertions.assertEquals("PK", schema.getColumns().get(0).getName());
            Assertions.assertEquals("ID1", schema.getColumns().get(1).getName());
            Assertions.assertEquals("ID2", schema.getColumns().get(2).getName());
            Assertions.assertEquals("ID3", schema.getColumns().get(3).getName());
            Assertions.assertEquals("NAME", schema.getColumns().get(4).getName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void testDropColumn() throws OdpsException {
        try {
            SchemaEvolutionUtils.dropColumn(
                    testOptions, TableId.tableId(TEST_TABLE), ImmutableList.of("ID1", "ID2"));
            TableSchema schema = odps.tables().get(TEST_TABLE).getSchema();

            Assertions.assertEquals(1, schema.getColumns().size());
            Assertions.assertEquals("PK", schema.getColumns().get(0).getName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void testRenameColumn() {
        try {
            TableSchema originSchema = odps.tables().get(TEST_TABLE).getSchema();
            Assertions.assertEquals("ID1", originSchema.getColumns().get(1).getName());
            Assertions.assertEquals("ID2", originSchema.getColumns().get(2).getName());

            SchemaEvolutionUtils.renameColumn(
                    testOptions,
                    TableId.tableId(TEST_TABLE),
                    ImmutableMap.of("ID1", "ID1_NEW", "ID2", "ID2_NEW"));

            TableSchema expectSchema = odps.tables().get(TEST_TABLE).getSchema();
            Assertions.assertEquals("ID1_NEW", expectSchema.getColumns().get(1).getName());
            Assertions.assertEquals("ID2_NEW", expectSchema.getColumns().get(2).getName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }
}
