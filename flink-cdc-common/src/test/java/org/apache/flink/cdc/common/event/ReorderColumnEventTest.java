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

package org.apache.flink.cdc.common.event;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReorderColumnEvent}. */
class ReorderColumnEventTest {

    @Test
    void testColumnMoveFactoriesUseTopLevelPositionAndReferenceName() {
        assertThat(ReorderColumnEvent.ColumnMove.first("first"))
                .isEqualTo(new ReorderColumnEvent.ColumnMove("first", ColumnPosition.FIRST, null));
        assertThat(ReorderColumnEvent.ColumnMove.last("last"))
                .isEqualTo(new ReorderColumnEvent.ColumnMove("last", ColumnPosition.LAST, null));
        assertThat(ReorderColumnEvent.ColumnMove.before("before", "reference"))
                .isEqualTo(
                        new ReorderColumnEvent.ColumnMove(
                                "before", ColumnPosition.BEFORE, "reference"));
        assertThat(ReorderColumnEvent.ColumnMove.after("after", "reference"))
                .isEqualTo(
                        new ReorderColumnEvent.ColumnMove(
                                "after", ColumnPosition.AFTER, "reference"));
        assertThat(
                        ReorderColumnEvent.ColumnMove.after("after", "reference")
                                .getReferenceColumnName())
                .isEqualTo("reference");
    }

    @Test
    void testRejectsStructurallyInvalidMovesAtConstructionTime() {
        TableId tableId = TableId.tableId("schema", "table");

        assertThatThrownBy(() -> new ReorderColumnEvent(tableId, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReorderColumnEvent(tableId, Collections.singletonList(null)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> new ReorderColumnEvent.ColumnMove(null, ColumnPosition.FIRST, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReorderColumnEvent.ColumnMove("column", null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new ReorderColumnEvent.ColumnMove(
                                        "column", ColumnPosition.BEFORE, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("referenceColumnName");
        assertThatThrownBy(
                        () ->
                                new ReorderColumnEvent.ColumnMove(
                                        "column", ColumnPosition.FIRST, "reference"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("referenceColumnName");
        assertThatThrownBy(
                        () ->
                                new ReorderColumnEvent.ColumnMove(
                                        "column", ColumnPosition.AFTER, "column"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testAllowsEmptyColumnMoveList() {
        ReorderColumnEvent event =
                new ReorderColumnEvent(TableId.tableId("schema", "table"), Collections.emptyList());

        assertThat(event.getColumnMoves()).isEmpty();
    }

    @Test
    void testRejectsDuplicateMovedColumnsAtEventConstructionTime() {
        TableId tableId = TableId.tableId("schema", "table");

        assertThatThrownBy(
                        () ->
                                new ReorderColumnEvent(
                                        tableId,
                                        java.util.Arrays.asList(
                                                ReorderColumnEvent.ColumnMove.first("id"),
                                                ReorderColumnEvent.ColumnMove.after("id", "name"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple moves for column id");
    }

    @Test
    void testRejectsMovesRelativeToMovedColumnsAtEventConstructionTime() {
        TableId tableId = TableId.tableId("schema", "table");

        assertThatThrownBy(
                        () ->
                                new ReorderColumnEvent(
                                        tableId,
                                        java.util.Arrays.asList(
                                                ReorderColumnEvent.ColumnMove.after("id", "name"),
                                                ReorderColumnEvent.ColumnMove.after(
                                                        "name", "age"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot move column id relative to moved column name");
    }

    @Test
    void testDefensivelyCopiesAndExposesImmutableCollections() {
        ReorderColumnEvent.ColumnMove move =
                new ReorderColumnEvent.ColumnMove("name", ColumnPosition.FIRST, null);
        List<ReorderColumnEvent.ColumnMove> moves = new ArrayList<>();
        moves.add(move);

        ReorderColumnEvent event =
                new ReorderColumnEvent(TableId.tableId("schema", "table"), moves);
        moves.clear();

        assertThat(event.getColumnMoves()).containsExactly(move);
        assertThatThrownBy(() -> event.getColumnMoves().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testCopyPreservesColumnMovesAndReplacesTableId() {
        TableId oldId = TableId.tableId("old");
        TableId newId = TableId.tableId("new");
        ReorderColumnEvent event =
                new ReorderColumnEvent(
                        oldId,
                        Collections.singletonList(ReorderColumnEvent.ColumnMove.first("name")));
        assertThat(event.copy(newId))
                .isEqualTo(new ReorderColumnEvent(newId, event.getColumnMoves()));
        assertThat(event.tableId()).isEqualTo(oldId);
    }
}
