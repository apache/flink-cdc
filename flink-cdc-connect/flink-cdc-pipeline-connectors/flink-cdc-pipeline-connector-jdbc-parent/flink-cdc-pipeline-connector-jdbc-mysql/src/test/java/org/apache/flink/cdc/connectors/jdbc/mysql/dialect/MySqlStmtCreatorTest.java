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

package org.apache.flink.cdc.connectors.jdbc.mysql.dialect;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link MySqlStmtCreatorFactory}. */
class MySqlStmtCreatorTest {
    @Test
    void testBuildCreateTableSql() {
        TableId tableId = TableId.tableId("test_schema", "test_table");
        boolean ignoreIfExists = true;

        Column column1 = Column.physicalColumn("id", DataTypes.INT(), "Primary Key");
        Column column2 = Column.physicalColumn("name", DataTypes.VARCHAR(32), "Name of the entity");
        Column column3 = Column.physicalColumn("age", DataTypes.INT(), null);

        List<Column> columns = Arrays.asList(column1, column2, column3);
        List<String> primaryKeys = Collections.singletonList("id");

        Schema schema = Schema.newBuilder().setColumns(columns).primaryKey(primaryKeys).build();

        String createTableSql =
                MySqlStmtCreatorFactory.INSTANCE.buildCreateTableSql(tableId, schema, true);

        String expectedSql =
                "CREATE TABLE IF NOT EXISTS test_schema.test_table (\n"
                        + "`id` INT COMMENT 'Primary Key',\n"
                        + "`name` VARCHAR(32) COMMENT 'Name of the entity',\n"
                        + "`age` INT,\n"
                        + "PRIMARY KEY (`id`)\n"
                        + ") ;";

        assertThat(createTableSql).isEqualTo(expectedSql);
    }

    @Test
    void testBuildCreateTableSqlWithoutPrimaryKey() {
        TableId tableId = TableId.tableId("test_schema", "test_table");
        boolean ignoreIfExists = true;

        Column column1 = Column.physicalColumn("id", DataTypes.INT(), "Primary Key");
        Column column2 = Column.physicalColumn("name", DataTypes.VARCHAR(32), "Name of the entity");
        Column column3 = Column.physicalColumn("age", DataTypes.INT(), null);

        List<Column> columns = Arrays.asList(column1, column2, column3);

        Schema schema = Schema.newBuilder().setColumns(columns).build();

        String createTableSql =
                MySqlStmtCreatorFactory.INSTANCE.buildCreateTableSql(tableId, schema, true);

        String expectedSql =
                "CREATE TABLE IF NOT EXISTS test_schema.test_table (\n"
                        + "`id` INT COMMENT 'Primary Key',\n"
                        + "`name` VARCHAR(32) COMMENT 'Name of the entity',\n"
                        + "`age` INT\n"
                        + ") ;";

        assertThat(createTableSql).isEqualTo(expectedSql);
    }

    @Test
    void testBuildDeleteSql() {
        TableId tableId = TableId.tableId("my_schema", "my_table");
        List<String> primaryKeys = Arrays.asList("id", "name");

        String expectedSql = "DELETE FROM my_schema.my_table WHERE id = ? AND name = ?";
        String actualSql = buildDeleteSql(tableId, primaryKeys);

        assertThat(actualSql).isEqualTo(expectedSql);
    }

    private String buildDeleteSql(TableId tableId, List<String> primaryKeys) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("DELETE FROM %s ", tableId.identifier()));
        builder.append("WHERE ");
        primaryKeys.forEach(pk -> builder.append(pk).append(" = ? AND "));
        // remove latest " AND "
        builder.setLength(builder.length() - 5);
        return builder.toString();
    }

    @Test
    void testBuildRenameColumnSql() {
        TableId tableId = TableId.tableId("test_schema", "test_table");
        String oldName = "old_name";
        String newName = "new_name";

        String renameColumnSql =
                MySqlStmtCreatorFactory.INSTANCE.buildRenameColumnSql(tableId, oldName, newName);
        String expectedSql =
                "ALTER TABLE test_schema.test_table RENAME COLUMN `old_name` TO `new_name`;";
        assertThat(renameColumnSql).isEqualTo(expectedSql);
    }

    @Test
    void testBuildRenameColumnSqlWithoutTypeChange() {
        TableId tableId = TableId.tableId("test_schema", "test_table");
        String oldColumnName = "description";
        String newColumnName = "details";

        String renameColumnSql =
                MySqlStmtCreatorFactory.INSTANCE.buildRenameColumnSql(
                        tableId, oldColumnName, newColumnName);

        String expectedSql =
                "ALTER TABLE test_schema.test_table RENAME COLUMN `description` TO `details`;";

        assertThat(renameColumnSql).isEqualTo(expectedSql);
    }

    @Test
    void testBuildAlterAddColumnsSql() {
        TableId tableId = TableId.tableId("test_schema", "test_table");

        Column column1 =
                Column.physicalColumn("new_column1", DataTypes.VARCHAR(255), "New column 1");
        Column column2 = Column.physicalColumn("new_column2", DataTypes.INT(), "New column 2");

        AddColumnEvent.ColumnWithPosition addColumn1 = AddColumnEvent.last(column1);
        AddColumnEvent.ColumnWithPosition addColumn2 = AddColumnEvent.first(column2);

        List<AddColumnEvent.ColumnWithPosition> addColumns = Arrays.asList(addColumn1, addColumn2);

        String alterAddColumnsSql =
                MySqlStmtCreatorFactory.INSTANCE.buildAlterAddColumnsSql(tableId, addColumns);

        String expectedSql =
                "ALTER TABLE test_schema.test_table ADD COLUMN `new_column1` VARCHAR(255) COMMENT 'New column 1', ADD COLUMN `new_column2` INT COMMENT 'New column 2' FIRST;";

        assertThat(alterAddColumnsSql).isEqualTo(expectedSql);
    }

    @Test
    void testBuildAlterAddColumnsSqlWithBeforePosition() {
        TableId tableId = TableId.tableId("test_schema", "test_table");

        Column column1 =
                Column.physicalColumn("new_column1", DataTypes.VARCHAR(255), "New column 1");
        Column column2 = Column.physicalColumn("new_column2", DataTypes.INT(), "New column 2");

        AddColumnEvent.ColumnWithPosition addColumn1 =
                AddColumnEvent.before(column1, "existing_column");
        AddColumnEvent.ColumnWithPosition addColumn2 =
                AddColumnEvent.after(column2, "another_column");

        List<AddColumnEvent.ColumnWithPosition> addColumns = Arrays.asList(addColumn1, addColumn2);

        String alterAddColumnsSql =
                MySqlStmtCreatorFactory.INSTANCE.buildAlterAddColumnsSql(tableId, addColumns);

        String expectedSql =
                "ALTER TABLE test_schema.test_table ADD COLUMN `new_column1` VARCHAR(255) COMMENT 'New column 1' BEFORE `existing_column`, ADD COLUMN `new_column2` INT COMMENT 'New column 2' AFTER `another_column`;";

        assertThat(alterAddColumnsSql).isEqualTo(expectedSql);
    }

    @Test
    void testGenerateUpsertQueryWithNamespaceAndSchema() {
        TableId tableId = TableId.tableId("test_schema", "test_table");
        Column column1 = Column.physicalColumn("id", DataTypes.INT(), "New column 1");
        Column column2 = Column.physicalColumn("name", DataTypes.VARCHAR(32), "New column 2");
        Column column3 = Column.physicalColumn("age", DataTypes.INT(), "New column 3");

        List<Column> columns = Arrays.asList(column1, column2, column3);

        String expectedQuery =
                "INSERT INTO test_schema.test_table (id, name, age) "
                        + "VALUES (?, ?, ?) "
                        + "ON DUPLICATE KEY UPDATE id = VALUES(id), name = VALUES(name), age = VALUES(age);";
        String actualQuery = MySqlStmtCreatorFactory.INSTANCE.buildUpsertSql(tableId, columns);

        assertThat(actualQuery).isEqualTo(expectedQuery);
    }
}
