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
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;

import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OracleAntlrDdlParserTest {

    private static final String DATABASE_NAME = "DLZSJT";
    private static final String SCHEMA_NAME = "WHZSJT";
    private static final TableId TABLE_ID = TableId.tableId(DATABASE_NAME, SCHEMA_NAME, "PRODUCTS");

    @Test
    void shouldIncludeDatabaseSchemaAndTableInAddColumnEvent() {
        SchemaChangeEvent event =
                parseSingleSchemaChangeEvent("ALTER TABLE PRODUCTS ADD COL_ADDED VARCHAR2(100)");

        assertThat(event).isInstanceOf(AddColumnEvent.class);
        assertThat(event.tableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void shouldIncludeDatabaseSchemaAndTableInDropColumnEvent() {
        SchemaChangeEvent event =
                parseSingleSchemaChangeEvent("ALTER TABLE PRODUCTS DROP COLUMN COL_DROPPED");

        assertThat(event).isInstanceOf(DropColumnEvent.class);
        assertThat(event.tableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void shouldIncludeDatabaseSchemaAndTableInRenameColumnEvent() {
        SchemaChangeEvent event =
                parseSingleSchemaChangeEvent(
                        "ALTER TABLE PRODUCTS RENAME COLUMN COL_OLD TO COL_NEW");

        assertThat(event).isInstanceOf(RenameColumnEvent.class);
        assertThat(event.tableId()).isEqualTo(TABLE_ID);
    }

    private static SchemaChangeEvent parseSingleSchemaChangeEvent(String ddl) {
        OracleAntlrDdlParser parser = new OracleAntlrDdlParser(DATABASE_NAME, SCHEMA_NAME);
        parser.setCurrentDatabase(DATABASE_NAME);
        parser.parse(ddl, new Tables());

        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();
        assertThat(events).hasSize(1);
        return events.get(0);
    }
}
