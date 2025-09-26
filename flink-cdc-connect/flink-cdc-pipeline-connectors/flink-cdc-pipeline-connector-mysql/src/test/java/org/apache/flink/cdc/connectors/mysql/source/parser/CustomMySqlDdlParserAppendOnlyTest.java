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

package org.apache.flink.cdc.connectors.mysql.source.parser;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CustomMySqlAntlrDdlParser} with append-only mode. */
class CustomMySqlDdlParserAppendOnlyTest {

    @Test
    void testCreateTableWithPrimaryKeyInNormalMode() {
        CustomMySqlAntlrDdlParser parser =
                new CustomMySqlAntlrDdlParser(false, false, false, false);
        Tables tables = new Tables();
        parser.setCurrentDatabase("test_db");

        String ddl =
                "CREATE TABLE test_table ("
                        + "id INT NOT NULL PRIMARY KEY, "
                        + "name VARCHAR(255), "
                        + "age INT"
                        + ");";

        parser.parse(ddl, tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);

        CreateTableEvent createEvent = (CreateTableEvent) events.get(0);
        assertThat(createEvent.getSchema().primaryKeys()).containsExactly("id");
    }

    @Test
    void testCreateTableWithPrimaryKeyInAppendOnlyMode() {
        CustomMySqlAntlrDdlParser parser = new CustomMySqlAntlrDdlParser(false, false, false, true);
        Tables tables = new Tables();
        parser.setCurrentDatabase("test_db");

        String ddl =
                "CREATE TABLE test_table ("
                        + "id INT NOT NULL PRIMARY KEY, "
                        + "name VARCHAR(255), "
                        + "age INT"
                        + ");";

        parser.parse(ddl, tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);

        CreateTableEvent createEvent = (CreateTableEvent) events.get(0);
        // In append-only mode, primary keys should be removed
        assertThat(createEvent.getSchema().primaryKeys()).isEmpty();
    }

    @Test
    void testCreateTableWithCompositePrimaryKeyInAppendOnlyMode() {
        CustomMySqlAntlrDdlParser parser = new CustomMySqlAntlrDdlParser(false, false, false, true);
        Tables tables = new Tables();
        parser.setCurrentDatabase("test_db");

        String ddl =
                "CREATE TABLE test_table ("
                        + "id INT NOT NULL, "
                        + "tenant_id INT NOT NULL, "
                        + "name VARCHAR(255), "
                        + "PRIMARY KEY (id, tenant_id)"
                        + ");";

        parser.parse(ddl, tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);

        CreateTableEvent createEvent = (CreateTableEvent) events.get(0);
        // In append-only mode, composite primary keys should also be removed
        assertThat(createEvent.getSchema().primaryKeys()).isEmpty();
    }

    @Test
    void testCreateTableLikeInAppendOnlyMode() {
        CustomMySqlAntlrDdlParser parser = new CustomMySqlAntlrDdlParser(false, false, false, true);
        Tables tables = new Tables();
        parser.setCurrentDatabase("test_db");

        // First create the source table with primary key
        String createSourceTable =
                "CREATE TABLE source_table ("
                        + "id INT NOT NULL PRIMARY KEY, "
                        + "name VARCHAR(255)"
                        + ");";
        parser.parse(createSourceTable, tables);
        parser.getAndClearParsedEvents(); // Clear the first event

        // Then create table like the source table
        String createLikeTable = "CREATE TABLE target_table LIKE source_table;";
        parser.parse(createLikeTable, tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);

        CreateTableEvent createEvent = (CreateTableEvent) events.get(0);
        // In append-only mode, primary keys should be removed even in LIKE tables
        assertThat(createEvent.getSchema().primaryKeys()).isEmpty();
    }
}
