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

package org.apache.flink.cdc.connectors.sqlserver.source.utils;

import org.apache.flink.table.api.ValidationException;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Types;

/** Unit tests for {@link SqlServerUtils#getSplitColumn}. */
class SqlServerUtilsTest {

    private static final TableId TABLE_ID = new TableId("testdb", "dbo", "customers");

    // Builds a table with columns: id (PK), name, created_at
    private static Table buildTable() {
        Column id =
                Column.editor()
                        .name("id")
                        .jdbcType(Types.INTEGER)
                        .type("INT", "INT")
                        .position(1)
                        .optional(false)
                        .create();
        Column name =
                Column.editor()
                        .name("name")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR", "VARCHAR(255)")
                        .position(2)
                        .optional(true)
                        .create();
        Column createdAt =
                Column.editor()
                        .name("created_at")
                        .jdbcType(Types.BIGINT)
                        .type("BIGINT", "BIGINT")
                        .position(3)
                        .optional(true)
                        .create();

        TableEditor editor = Table.editor().tableId(TABLE_ID);
        editor.addColumn(id);
        editor.addColumn(name);
        editor.addColumn(createdAt);
        editor.setPrimaryKeyNames("id");
        return editor.create();
    }

    // Builds a table with no primary key, columns: name, created_at
    private static Table buildTableWithoutPrimaryKey() {
        Column name =
                Column.editor()
                        .name("name")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR", "VARCHAR(255)")
                        .position(1)
                        .optional(true)
                        .create();
        Column createdAt =
                Column.editor()
                        .name("created_at")
                        .jdbcType(Types.BIGINT)
                        .type("BIGINT", "BIGINT")
                        .position(2)
                        .optional(true)
                        .create();

        TableEditor editor = Table.editor().tableId(TABLE_ID);
        editor.addColumn(name);
        editor.addColumn(createdAt);
        return editor.create();
    }

    @Test
    void testGetSplitColumnDefaultsToPrimaryKey() {
        Table table = buildTable();
        Column splitColumn = SqlServerUtils.getSplitColumn(table, null);
        Assertions.assertThat(splitColumn.name()).isEqualTo("id");
    }

    @Test
    void testGetSplitColumnWithPrimaryKeyColumnExplicitlySet() {
        Table table = buildTable();
        Column splitColumn = SqlServerUtils.getSplitColumn(table, "id");
        Assertions.assertThat(splitColumn.name()).isEqualTo("id");
    }

    @Test
    void testGetSplitColumnWithNonPrimaryKeyColumn() {
        Table table = buildTable();
        // Before the fix, this threw: "Chunk key column 'created_at' doesn't exist in the
        // primary key [id]"
        Column splitColumn = SqlServerUtils.getSplitColumn(table, "created_at");
        Assertions.assertThat(splitColumn.name()).isEqualTo("created_at");
    }

    @Test
    void testGetSplitColumnWithNonPrimaryKeyColumnByName() {
        Table table = buildTable();
        Column splitColumn = SqlServerUtils.getSplitColumn(table, "name");
        Assertions.assertThat(splitColumn.name()).isEqualTo("name");
    }

    @Test
    void testGetSplitColumnWithChunkKeyOnTableWithoutPrimaryKey() {
        Table table = buildTableWithoutPrimaryKey();
        // Before the fix, this threw immediately because primaryKeys.isEmpty() with no chunkKey
        // guard. Now it should succeed when chunkKeyColumn is provided.
        Column splitColumn = SqlServerUtils.getSplitColumn(table, "created_at");
        Assertions.assertThat(splitColumn.name()).isEqualTo("created_at");
    }

    @Test
    void testGetSplitColumnThrowsWhenNoPrimaryKeyAndNoChunkKeyColumn() {
        Table table = buildTableWithoutPrimaryKey();
        Assertions.assertThatThrownBy(() -> SqlServerUtils.getSplitColumn(table, null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't have primary key");
    }

    @Test
    void testGetSplitColumnThrowsWhenChunkKeyColumnDoesNotExist() {
        Table table = buildTable();
        Assertions.assertThatThrownBy(
                        () -> SqlServerUtils.getSplitColumn(table, "nonexistent_column"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't exist in the columns")
                .hasMessageContaining("nonexistent_column");
    }
}
