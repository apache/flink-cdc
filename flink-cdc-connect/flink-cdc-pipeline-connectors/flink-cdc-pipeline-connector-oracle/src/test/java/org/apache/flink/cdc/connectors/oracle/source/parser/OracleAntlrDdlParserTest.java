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

package org.apache.flink.cdc.connectors.oracle.source.parser;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OracleAntlrDdlParser}. */
class OracleAntlrDdlParserTest {

    private static final String DATABASE_NAME = "XE";
    private static final String SCHEMA_NAME = "DEBEZIUM";
    private static final String TABLE_NAME = "PRODUCTS";
    private static final io.debezium.relational.TableId DBZ_TABLE_ID =
            new io.debezium.relational.TableId(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME);
    private static final TableId CDC_TABLE_ID = TableId.tableId(SCHEMA_NAME, TABLE_NAME);

    @Test
    void shouldParseAddDateColumnAsTimestamp() {
        AddColumnEvent event =
                parseSingleAddColumnEvent(
                        "ALTER TABLE PRODUCTS ADD CREATED_AT DATE", productsTable());

        assertThat(event.tableId()).isEqualTo(CDC_TABLE_ID);
        assertThat(event.getAddedColumns())
                .containsExactly(
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("CREATED_AT", DataTypes.TIMESTAMP())));
    }

    @Test
    void shouldParseAddLongColumnAsString() {
        AddColumnEvent event =
                parseSingleAddColumnEvent(
                        "ALTER TABLE PRODUCTS ADD DESCRIPTION LONG", productsTable());

        assertThat(event.tableId()).isEqualTo(CDC_TABLE_ID);
        assertThat(event.getAddedColumns())
                .containsExactly(
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("DESCRIPTION", DataTypes.STRING())));
    }

    @Test
    void shouldParseModifyDateColumnAsTimestampAlterEvent() {
        AlterColumnTypeEvent event =
                parseSingleAlterColumnTypeEvent(
                        "ALTER TABLE PRODUCTS MODIFY CREATED_AT DATE DEFAULT NULL",
                        productsTable(dateColumn("CREATED_AT", "TIMESTAMP", Types.TIMESTAMP, 6)));

        assertThat(event.tableId()).isEqualTo(CDC_TABLE_ID);
        assertThat(event.getTypeMapping()).containsEntry("CREATED_AT", DataTypes.TIMESTAMP());
    }

    @Test
    void shouldParseModifyLongColumnAsStringAlterEvent() {
        AlterColumnTypeEvent event =
                parseSingleAlterColumnTypeEvent(
                        "ALTER TABLE PRODUCTS MODIFY DESCRIPTION LONG DEFAULT NULL",
                        productsTable(varcharColumn("DESCRIPTION", 128)));

        assertThat(event.tableId()).isEqualTo(CDC_TABLE_ID);
        assertThat(event.getTypeMapping()).containsEntry("DESCRIPTION", DataTypes.STRING());
    }

    private static AddColumnEvent parseSingleAddColumnEvent(String ddl, Tables tables) {
        List<SchemaChangeEvent> events = parseEvents(ddl, tables);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AddColumnEvent.class);
        return (AddColumnEvent) events.get(0);
    }

    private static AlterColumnTypeEvent parseSingleAlterColumnTypeEvent(String ddl, Tables tables) {
        List<SchemaChangeEvent> events = parseEvents(ddl, tables);

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        return (AlterColumnTypeEvent) events.get(0);
    }

    private static List<SchemaChangeEvent> parseEvents(String ddl, Tables tables) {
        OracleAntlrDdlParser parser = new OracleAntlrDdlParser(DATABASE_NAME, SCHEMA_NAME);
        parser.setCurrentDatabase(DATABASE_NAME);
        parser.setCurrentSchema(SCHEMA_NAME);
        parser.parse(ddl, tables);
        return parser.getAndClearParsedEvents();
    }

    private static Tables productsTable(io.debezium.relational.Column... columns) {
        Tables tables = new Tables();
        io.debezium.relational.TableEditor tableEditor = Table.editor().tableId(DBZ_TABLE_ID);
        for (io.debezium.relational.Column column : columns) {
            tableEditor.addColumn(column);
        }
        tables.overwriteTable(tableEditor.create());
        return tables;
    }

    private static io.debezium.relational.Column varcharColumn(String name, int length) {
        return io.debezium.relational.Column.editor()
                .name(name)
                .jdbcType(Types.VARCHAR)
                .type("VARCHAR2", "VARCHAR2")
                .length(length)
                .optional(true)
                .create();
    }

    private static io.debezium.relational.Column dateColumn(
            String name, String typeName, int jdbcType, int length) {
        return io.debezium.relational.Column.editor()
                .name(name)
                .jdbcType(jdbcType)
                .type(typeName, typeName)
                .length(length)
                .optional(true)
                .create();
    }
}
