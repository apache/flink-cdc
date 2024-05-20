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
import org.apache.flink.cdc.connectors.maxcompute.MockedMaxComputeOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.aliyun.odps.OdpsException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;

/** e2e test of SchemaEvolutionUtils. */
public class SchemaEvolutionUtilsTest {

    private static final String TEST_TABLE = "schema_evolution_test";

    @BeforeClass
    static void testCreateTable() {
        try {
            SchemaEvolutionUtils.createTable(
                    MockedMaxComputeOptions.INSTANCE,
                    TableId.tableId(TEST_TABLE),
                    Schema.newBuilder()
                            .physicalColumn("pk", DataTypes.BIGINT())
                            .physicalColumn("id", DataTypes.BIGINT())
                            .primaryKey("pk")
                            .build());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    void testAddColumn() throws OdpsException {
        try {
            SchemaEvolutionUtils.addColumns(
                    MockedMaxComputeOptions.INSTANCE,
                    TableId.tableId(TEST_TABLE),
                    ImmutableList.of(
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("id2", DataTypes.BIGINT())),
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("id3", DataTypes.BIGINT())),
                            new AddColumnEvent.ColumnWithPosition(
                                    Column.physicalColumn("name", DataTypes.STRING()))));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    void testDropColumn() throws OdpsException {
        try {
            SchemaEvolutionUtils.dropColumn(
                    MockedMaxComputeOptions.INSTANCE,
                    TableId.tableId(TEST_TABLE),
                    ImmutableList.of("id", "name"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    void testAlterColumnType() {
        try {
            SchemaEvolutionUtils.alterColumnType(
                    MockedMaxComputeOptions.INSTANCE,
                    TableId.tableId(TEST_TABLE),
                    ImmutableMap.of("id2", DataTypes.STRING(), "id3", DataTypes.STRING()));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
