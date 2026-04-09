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

package org.apache.flink.cdc.connectors.oracle.utils;

import org.apache.flink.cdc.common.schema.Schema;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OracleSchemaUtils}. */
class OracleSchemaUtilsTest {

    @Test
    void shouldPreserveQuotedIdentifierCase() {
        assertThat(OracleSchemaUtils.removeQuotes("\"CamelCase\"")).isEqualTo("CamelCase");
    }

    @Test
    void shouldUppercaseUnquotedIdentifier() {
        assertThat(OracleSchemaUtils.removeQuotes("order_id")).isEqualTo("ORDER_ID");
    }

    @Test
    void shouldNormalizeQuotedColumnNameWhenConvertingColumn() {
        Column quotedColumn =
                Column.editor()
                        .name("\"CamelCase\"")
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER", "NUMBER")
                        .optional(false)
                        .create();

        assertThat(OracleSchemaUtils.toColumn(quotedColumn).getName()).isEqualTo("CamelCase");
    }

    @Test
    void shouldNormalizeQuotedIdentifiersWhenConvertingSchema() {
        Table table =
                createTable(
                        "DEBEZIUM",
                        "ORDERS",
                        new String[] {"\"Id\"", "\"CamelCase\"", "ORDER_ID"},
                        "\"Id\"");

        Schema schema = OracleSchemaUtils.toSchema(table);

        assertThat(schema.getColumnNames()).containsExactly("Id", "CamelCase", "ORDER_ID");
        assertThat(schema.primaryKeys()).containsExactly("Id");
    }

    private static Table createTable(
            String schemaName, String tableName, String[] columnNames, String primaryKeyName) {
        TableEditor tableEditor =
                Table.editor()
                        .tableId(new io.debezium.relational.TableId(null, schemaName, tableName));
        Arrays.stream(columnNames)
                .map(OracleSchemaUtilsTest::createColumn)
                .forEach(tableEditor::addColumn);
        tableEditor.setPrimaryKeyNames(Arrays.asList(primaryKeyName));
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
}
