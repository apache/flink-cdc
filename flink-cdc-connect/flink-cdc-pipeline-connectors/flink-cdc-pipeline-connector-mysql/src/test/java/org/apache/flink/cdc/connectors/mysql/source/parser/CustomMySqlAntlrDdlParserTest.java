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

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests MySQL copy-table DDL handling in {@link CustomMySqlAntlrDdlParser}. */
class CustomMySqlAntlrDdlParserTest {

    private static final TableId TARGET_TABLE = new TableId("inventory", null, "target_table");
    private static final TableId TEMPLATE_TABLE = new TableId("inventory", null, "template_table");

    @Test
    void shouldPreserveExistingSchemaForNoOpCopyCreateTable() {
        Tables tables = new Tables();
        CustomMySqlAntlrDdlParser parser = createParser();
        parser.parse(
                "CREATE TABLE inventory.target_table ("
                        + "id BIGINT NOT NULL, name VARCHAR(32), status INT, PRIMARY KEY (id));"
                        + "CREATE TABLE inventory.template_table ("
                        + "id BIGINT NOT NULL, revision BIGINT, name VARCHAR(64), status INT, "
                        + "PRIMARY KEY (id));",
                tables);
        parser.getAndClearParsedEvents();

        parser.parse(
                "CREATE TABLE IF NOT EXISTS inventory.target_table "
                        + "LIKE inventory.template_table;",
                tables);

        Table target = tables.forTable(TARGET_TABLE);
        assertThat(target).isNotNull();
        assertThat(target.retrieveColumnNames()).containsExactly("id", "name", "status");
        assertThat(target.columnWithName("name").length()).isEqualTo(32);
        assertThat(target.primaryKeyColumnNames()).containsExactly("id");
        assertThat(parser.getAndClearParsedEvents()).isEmpty();
    }

    @Test
    void shouldCopySchemaWhenTargetDoesNotExist() {
        Tables tables = new Tables();
        CustomMySqlAntlrDdlParser parser = createParser();
        parser.parse(
                "CREATE TABLE inventory.template_table ("
                        + "id BIGINT NOT NULL, revision BIGINT, name VARCHAR(64), status INT, "
                        + "PRIMARY KEY (id));",
                tables);
        parser.getAndClearParsedEvents();

        parser.parse(
                "CREATE TABLE IF NOT EXISTS inventory.target_table "
                        + "LIKE inventory.template_table;",
                tables);

        Table target = tables.forTable(TARGET_TABLE);
        Table template = tables.forTable(TEMPLATE_TABLE);
        assertThat(target).isNotNull();
        assertThat(target.retrieveColumnNames())
                .containsExactlyElementsOf(template.retrieveColumnNames());
        assertThat(target.primaryKeyColumnNames())
                .containsExactlyElementsOf(template.primaryKeyColumnNames());
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
    }

    private CustomMySqlAntlrDdlParser createParser() {
        return new CustomMySqlAntlrDdlParser(false, false, false);
    }
}
