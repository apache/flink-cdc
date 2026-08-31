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

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.ColumnPosition;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import io.debezium.relational.Tables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CustomAlterTableParserListener} reorder event emission. */
class CustomAlterTableParserListenerTest {

    private CustomMySqlAntlrDdlParser parser;
    private Tables tables;

    @BeforeEach
    void setUp() {
        parser =
                new CustomMySqlAntlrDdlParser(
                        /* includeComments */ true,
                        /* tinyInt1isBit */ false,
                        /* isTableIdCaseInsensitive */ false);
        tables = new Tables();
        parser.setCurrentDatabase("test_db");
        parser.parse(
                "CREATE TABLE t (id INT NOT NULL, name VARCHAR(17), age INT, PRIMARY KEY(id))",
                tables);
        parser.getAndClearParsedEvents();
    }

    @Test
    void testModifyColumnFirst() {
        parser.parse("ALTER TABLE t MODIFY COLUMN name VARCHAR(34) FIRST", tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        assertThat(((AlterColumnTypeEvent) events.get(0)).getTypeMapping())
                .containsEntry("name", DataTypes.VARCHAR(34));
        assertThat(events.get(1)).isInstanceOf(ReorderColumnEvent.class);

        ReorderColumnEvent reorderEvent = (ReorderColumnEvent) events.get(1);
        assertThat(reorderEvent.getColumnMoves()).hasSize(1);
        ReorderColumnEvent.ColumnMove move = reorderEvent.getColumnMoves().get(0);
        assertThat(move.getColumnName()).isEqualTo("name");
        assertThat(move.getPosition()).isEqualTo(ColumnPosition.FIRST);
        assertThat(move.getReferenceColumnName()).isNull();
    }

    @Test
    void testModifyColumnAfter() {
        parser.parse("ALTER TABLE t MODIFY COLUMN name VARCHAR(34) AFTER id", tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        assertThat(((AlterColumnTypeEvent) events.get(0)).getTypeMapping())
                .containsEntry("name", DataTypes.VARCHAR(34));
        assertThat(events.get(1)).isInstanceOf(ReorderColumnEvent.class);

        ReorderColumnEvent reorderEvent = (ReorderColumnEvent) events.get(1);
        ReorderColumnEvent.ColumnMove move = reorderEvent.getColumnMoves().get(0);
        assertThat(move.getColumnName()).isEqualTo("name");
        assertThat(move.getPosition()).isEqualTo(ColumnPosition.AFTER);
        assertThat(move.getReferenceColumnName()).isEqualTo("id");
    }

    @Test
    void testModifyColumnWithoutPosition() {
        parser.parse("ALTER TABLE t MODIFY COLUMN name VARCHAR(34)", tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
    }

    @Test
    void testChangeColumnFirstWithRename() {
        parser.parse("ALTER TABLE t CHANGE COLUMN name full_name VARCHAR(34) FIRST", tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        assertThat(((AlterColumnTypeEvent) events.get(0)).getTypeMapping())
                .containsEntry("name", DataTypes.VARCHAR(34));
        assertThat(events.get(1)).isInstanceOf(RenameColumnEvent.class);
        assertThat(((RenameColumnEvent) events.get(1)).getNameMapping())
                .containsEntry("name", "full_name");
        assertThat(events.get(2)).isInstanceOf(ReorderColumnEvent.class);

        ReorderColumnEvent reorderEvent = (ReorderColumnEvent) events.get(2);
        ReorderColumnEvent.ColumnMove move = reorderEvent.getColumnMoves().get(0);
        assertThat(move.getColumnName()).isEqualTo("full_name");
        assertThat(move.getPosition()).isEqualTo(ColumnPosition.FIRST);
        assertThat(move.getReferenceColumnName()).isNull();
    }

    @Test
    void testChangeColumnAfterWithRename() {
        parser.parse("ALTER TABLE t CHANGE COLUMN name full_name VARCHAR(34) AFTER id", tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        assertThat(((AlterColumnTypeEvent) events.get(0)).getTypeMapping())
                .containsEntry("name", DataTypes.VARCHAR(34));
        assertThat(events.get(1)).isInstanceOf(RenameColumnEvent.class);
        assertThat(((RenameColumnEvent) events.get(1)).getNameMapping())
                .containsEntry("name", "full_name");
        assertThat(events.get(2)).isInstanceOf(ReorderColumnEvent.class);

        ReorderColumnEvent reorderEvent = (ReorderColumnEvent) events.get(2);
        ReorderColumnEvent.ColumnMove move = reorderEvent.getColumnMoves().get(0);
        assertThat(move.getColumnName()).isEqualTo("full_name");
        assertThat(move.getPosition()).isEqualTo(ColumnPosition.AFTER);
        assertThat(move.getReferenceColumnName()).isEqualTo("id");
    }

    @Test
    void testChangeColumnFirstWithoutRename() {
        parser.parse("ALTER TABLE t CHANGE COLUMN name name VARCHAR(34) FIRST", tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        assertThat(((AlterColumnTypeEvent) events.get(0)).getTypeMapping())
                .containsEntry("name", DataTypes.VARCHAR(34));
        assertThat(events.get(1)).isInstanceOf(ReorderColumnEvent.class);

        ReorderColumnEvent reorderEvent = (ReorderColumnEvent) events.get(1);
        ReorderColumnEvent.ColumnMove move = reorderEvent.getColumnMoves().get(0);
        assertThat(move.getColumnName()).isEqualTo("name");
        assertThat(move.getPosition()).isEqualTo(ColumnPosition.FIRST);
        assertThat(move.getReferenceColumnName()).isNull();
    }

    @Test
    void testChangeColumnWithoutPosition() {
        parser.parse("ALTER TABLE t CHANGE COLUMN name full_name VARCHAR(34)", tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(AlterColumnTypeEvent.class);
        assertThat(events.get(1)).isInstanceOf(RenameColumnEvent.class);
    }

    @Test
    void testCompoundModifyAndChangeColumnWithPosition() {
        parser.parse(
                "ALTER TABLE t "
                        + "MODIFY COLUMN age BIGINT FIRST, "
                        + "CHANGE COLUMN name full_name VARCHAR(34) AFTER age",
                tables);
        List<SchemaChangeEvent> events =
                parser.getAndClearParsedEvents().stream().collect(Collectors.toList());

        assertThat(events)
                .extracting(Object::getClass)
                .containsExactly(
                        AlterColumnTypeEvent.class,
                        ReorderColumnEvent.class,
                        AlterColumnTypeEvent.class,
                        RenameColumnEvent.class,
                        ReorderColumnEvent.class);

        assertThat(((AlterColumnTypeEvent) events.get(0)).getTypeMapping())
                .containsExactlyEntriesOf(Collections.singletonMap("age", DataTypes.BIGINT()));
        ReorderColumnEvent firstReorder = (ReorderColumnEvent) events.get(1);
        assertThat(firstReorder.getColumnMoves()).hasSize(1);
        assertThat(firstReorder.getColumnMoves().get(0).getColumnName()).isEqualTo("age");
        assertThat(firstReorder.getColumnMoves().get(0).getPosition())
                .isEqualTo(ColumnPosition.FIRST);
        assertThat(firstReorder.getColumnMoves().get(0).getReferenceColumnName()).isNull();

        assertThat(((AlterColumnTypeEvent) events.get(2)).getTypeMapping())
                .containsExactlyEntriesOf(Collections.singletonMap("name", DataTypes.VARCHAR(34)));
        assertThat(((RenameColumnEvent) events.get(3)).getNameMapping())
                .containsExactlyEntriesOf(Collections.singletonMap("name", "full_name"));
        ReorderColumnEvent afterReorder = (ReorderColumnEvent) events.get(4);
        assertThat(afterReorder.getColumnMoves()).hasSize(1);
        assertThat(afterReorder.getColumnMoves().get(0).getColumnName()).isEqualTo("full_name");
        assertThat(afterReorder.getColumnMoves().get(0).getPosition())
                .isEqualTo(ColumnPosition.AFTER);
        assertThat(afterReorder.getColumnMoves().get(0).getReferenceColumnName()).isEqualTo("age");

        assertThat(events)
                .extracting(SchemaChangeEvent::tableId)
                .containsOnly(TableId.tableId("test_db", "t"));

        Schema evolvedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey(Arrays.asList("id"))
                        .build();
        for (SchemaChangeEvent event : events) {
            evolvedSchema = SchemaUtils.applySchemaChangeEvent(evolvedSchema, event);
        }

        assertThat(evolvedSchema.getColumnNames()).containsExactly("age", "full_name", "id");
        assertThat(evolvedSchema.getColumn("age").get().getType()).isEqualTo(DataTypes.BIGINT());
        assertThat(evolvedSchema.getColumn("full_name").get().getType())
                .isEqualTo(DataTypes.VARCHAR(34));
    }

    @Test
    void testCompoundModifyAndChangeWithoutColumnKeyword() {
        parser.parse(
                "ALTER TABLE t "
                        + "MODIFY age BIGINT FIRST, "
                        + "CHANGE name full_name VARCHAR(34) AFTER age",
                tables);
        List<SchemaChangeEvent> events = parser.getAndClearParsedEvents();

        assertThat(events)
                .extracting(Object::getClass)
                .containsExactly(
                        AlterColumnTypeEvent.class,
                        ReorderColumnEvent.class,
                        AlterColumnTypeEvent.class,
                        RenameColumnEvent.class,
                        ReorderColumnEvent.class);

        ReorderColumnEvent firstReorder = (ReorderColumnEvent) events.get(1);
        assertThat(firstReorder.getColumnMoves())
                .containsExactly(
                        new ReorderColumnEvent.ColumnMove("age", ColumnPosition.FIRST, null));

        ReorderColumnEvent afterReorder = (ReorderColumnEvent) events.get(4);
        assertThat(afterReorder.getColumnMoves())
                .containsExactly(
                        new ReorderColumnEvent.ColumnMove(
                                "full_name", ColumnPosition.AFTER, "age"));

        Schema evolvedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey(Arrays.asList("id"))
                        .build();
        for (SchemaChangeEvent event : events) {
            evolvedSchema = SchemaUtils.applySchemaChangeEvent(evolvedSchema, event);
        }

        assertThat(evolvedSchema.getColumnNames()).containsExactly("age", "full_name", "id");
        assertThat(evolvedSchema.getColumn("age").get().getType()).isEqualTo(DataTypes.BIGINT());
        assertThat(evolvedSchema.getColumn("full_name").get().getType())
                .isEqualTo(DataTypes.VARCHAR(34));
    }

    @Test
    void testReorderNormalizesCaseForMovedAndReferenceColumns() {
        parser = new CustomMySqlAntlrDdlParser(true, false, true);
        tables = new Tables();
        parser.setCurrentDatabase("test_db");
        parser.parse("CREATE TABLE t (ID INT, NAME VARCHAR(17), AGE INT)", tables);
        parser.getAndClearParsedEvents();
        parser.parse(
                "ALTER TABLE t MODIFY COLUMN AGE INT AFTER ID, "
                        + "CHANGE COLUMN NAME FULL_NAME VARCHAR(34) AFTER AGE",
                tables);
        List<ReorderColumnEvent> reorders =
                parser.getAndClearParsedEvents().stream()
                        .filter(ReorderColumnEvent.class::isInstance)
                        .map(ReorderColumnEvent.class::cast)
                        .collect(Collectors.toList());
        assertThat(reorders)
                .containsExactly(
                        new ReorderColumnEvent(
                                TableId.tableId("test_db", "t"),
                                Collections.singletonList(
                                        ReorderColumnEvent.ColumnMove.after("age", "id"))),
                        new ReorderColumnEvent(
                                TableId.tableId("test_db", "t"),
                                Collections.singletonList(
                                        ReorderColumnEvent.ColumnMove.after("full_name", "age"))));
    }
}
