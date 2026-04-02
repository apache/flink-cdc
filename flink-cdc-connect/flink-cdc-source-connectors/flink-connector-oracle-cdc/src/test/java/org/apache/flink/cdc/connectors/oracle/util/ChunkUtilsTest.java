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

package org.apache.flink.cdc.connectors.oracle.util;

import org.apache.flink.cdc.connectors.base.utils.SplitKeyUtils;
import org.apache.flink.table.api.ValidationException;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import oracle.sql.ROWID;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ChunkUtils}. */
class ChunkUtilsTest {

    @Test
    void testGetChunkKeyColumnSupportsLegacySingleColumnConfig() {
        Table table = createTable("DEBEZIUM", "PRODUCTS", "ID", "ORDER_ID");

        assertThat(ChunkUtils.getChunkKeyColumn(table, "ORDER_ID").name()).isEqualTo("ORDER_ID");
    }

    @Test
    void testGetChunkKeyColumnResolvesPerTableMapping() {
        Table table = createTable("DEBEZIUM", "PRODUCTS", "ID", "ORDER_ID");

        assertThat(
                        ChunkUtils.getChunkKeyColumn(
                                        table, "debezium.products:ORDER_ID;debezium.category:ID")
                                .name())
                .isEqualTo("ORDER_ID");
    }

    @Test
    void testGetChunkKeyColumnFallsBackToRowIdWhenNoMappingMatches() {
        Table table = createTable("DEBEZIUM", "PRODUCTS", "ID", "ORDER_ID");

        assertThat(
                        ChunkUtils.getChunkKeyColumn(
                                        table, "debezium.category:ID;debezium.orders:ORDER_ID")
                                .name())
                .isEqualTo(ROWID.class.getSimpleName());
    }

    @Test
    void testGetChunkKeyColumnFallsBackToRowIdWithoutPrimaryKey() {
        Table table = createTableWithoutPrimaryKey("DEBEZIUM", "PRODUCTS", "ID", "ORDER_ID");

        assertThat(ChunkUtils.getChunkKeyColumn(table, null).name())
                .isEqualTo(ROWID.class.getSimpleName());
    }

    @Test
    void testSplitKeyRangeContainsUsesOracleRowIdAlphabetOrder() throws Exception {
        ROWID rowIdWithinRange = new ROWID("AAAzIdACKAAABWCAAA");
        ROWID rangeEnd = new ROWID("AAAzIdAC/AACWIPAAB");

        assertThat(
                        SplitKeyUtils.splitKeyRangeContains(
                                new Object[] {rowIdWithinRange}, null, new Object[] {rangeEnd}))
                .isTrue();
        assertThat(
                        SplitKeyUtils.splitKeyRangeContains(
                                new Object[] {rangeEnd}, null, new Object[] {rangeEnd}))
                .isFalse();
    }

    @Test
    void testSplitKeyRangeContainsUsesOracleRowIdBoundaryCharacterOrder() throws Exception {
        assertOracleRowIdOrdering("Z", "a");
        assertOracleRowIdOrdering("z", "0");
        assertOracleRowIdOrdering("9", "+");
        assertOracleRowIdOrdering("+", "/");
    }

    @Test
    void testGetChunkKeyColumnRejectsMalformedMapping() {
        Table table = createTable("DEBEZIUM", "PRODUCTS", "ID", "ORDER_ID");

        assertThatThrownBy(
                        () ->
                                ChunkUtils.getChunkKeyColumn(
                                        table, "debezium.products:ORDER_ID:EXTRA"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("failed to parse");
    }

    private static Table createTable(String schemaName, String tableName, String... columnNames) {
        TableEditor tableEditor =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId(null, schemaName, tableName));
        Arrays.stream(columnNames)
                .map(ChunkUtilsTest::createColumn)
                .forEach(tableEditor::addColumn);
        tableEditor.setPrimaryKeyNames(Arrays.asList("ID"));
        return tableEditor.create();
    }

    private static Table createTableWithoutPrimaryKey(
            String schemaName, String tableName, String... columnNames) {
        TableEditor tableEditor =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId(null, schemaName, tableName));
        Arrays.stream(columnNames)
                .map(ChunkUtilsTest::createColumn)
                .forEach(tableEditor::addColumn);
        return tableEditor.create();
    }

    private static Column createColumn(String columnName) {
        return Column.editor()
                .name(columnName)
                .jdbcType(Types.NUMERIC)
                .type("NUMBER", "NUMBER")
                .optional(false)
                .create();
    }

    private static void assertOracleRowIdOrdering(String lowerSuffix, String upperSuffix)
            throws Exception {
        ROWID lower = new ROWID("AAAzIdACKAAABWCAA" + lowerSuffix);
        ROWID upper = new ROWID("AAAzIdACKAAABWCAA" + upperSuffix);

        assertThat(
                        SplitKeyUtils.splitKeyRangeContains(
                                new Object[] {lower}, null, new Object[] {upper}))
                .as("expected ROWID %s to be before %s", lower, upper)
                .isTrue();
        assertThat(
                        SplitKeyUtils.splitKeyRangeContains(
                                new Object[] {upper}, null, new Object[] {upper}))
                .as("upper boundary must remain exclusive for ROWID %s", upper)
                .isFalse();
    }
}
