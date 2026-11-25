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

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link SchemaChangeEvent} that represents an {@code ALTER TABLE ... REORDER COLUMN} DDL.
 *
 * <p>Column moves are applied sequentially in list order, with each move observing the schema
 * produced by the preceding move. A column may be moved at most once in one event. A {@link
 * ColumnPosition#BEFORE} or {@link ColumnPosition#AFTER} move must reference a stable anchor that
 * is not moved by the same event and exists in the schema when the event is applied. An empty move
 * list is a valid no-op.
 *
 * <p>For example, applying {@code [ColumnMove.after("a", "id"), ColumnMove.after("c", "id")]} to
 * {@code [id, a, b, c]} produces {@code [id, c, a, b]}. Reversing the move list produces a
 * different result.
 */
@PublicEvolving
public final class ReorderColumnEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final List<ColumnMove> columnMoves;

    public ReorderColumnEvent(TableId tableId, List<ColumnMove> columnMoves) {
        this.tableId = Preconditions.checkNotNull(tableId);
        Preconditions.checkArgument(columnMoves != null, "columnMoves must not be null");
        for (ColumnMove columnMove : columnMoves) {
            Preconditions.checkArgument(columnMove != null, "Column move must not be null");
        }
        this.columnMoves = Collections.unmodifiableList(new ArrayList<>(columnMoves));
        validateColumnMoves();
    }

    private void validateColumnMoves() {
        Set<String> movedColumns = new HashSet<>();
        for (ColumnMove move : columnMoves) {
            Preconditions.checkArgument(
                    movedColumns.add(move.getColumnName()),
                    "ReorderColumnEvent contains multiple moves for column %s",
                    move.getColumnName());
        }

        for (ColumnMove move : columnMoves) {
            String referencedColumn = move.getReferenceColumnName();
            if (referencedColumn != null) {
                Preconditions.checkArgument(
                        !movedColumns.contains(referencedColumn),
                        "ReorderColumnEvent cannot move column %s relative to moved column %s",
                        move.getColumnName(),
                        referencedColumn);
            }
        }
    }

    public List<ColumnMove> getColumnMoves() {
        return columnMoves;
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.REORDER_COLUMN;
    }

    @Override
    public SchemaChangeEvent copy(TableId newTableId) {
        return new ReorderColumnEvent(newTableId, columnMoves);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReorderColumnEvent)) {
            return false;
        }
        ReorderColumnEvent that = (ReorderColumnEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(columnMoves, that.columnMoves);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, columnMoves);
    }

    @Override
    public String toString() {
        return "ReorderColumnEvent{" + "tableId=" + tableId + ", columnMoves=" + columnMoves + '}';
    }

    /** Represents moving an existing column to a new position. */
    public static final class ColumnMove implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String columnName;
        private final ColumnPosition position;
        private final @Nullable String referenceColumnName;

        /** Creates a move to the first column position. */
        public static ColumnMove first(String columnName) {
            return new ColumnMove(columnName, ColumnPosition.FIRST, null);
        }

        /** Creates a move to the last column position. */
        public static ColumnMove last(String columnName) {
            return new ColumnMove(columnName, ColumnPosition.LAST, null);
        }

        /** Creates a move before the reference column. */
        public static ColumnMove before(String columnName, String referenceColumnName) {
            return new ColumnMove(columnName, ColumnPosition.BEFORE, referenceColumnName);
        }

        /** Creates a move after the reference column. */
        public static ColumnMove after(String columnName, String referenceColumnName) {
            return new ColumnMove(columnName, ColumnPosition.AFTER, referenceColumnName);
        }

        public ColumnMove(
                String columnName, ColumnPosition position, @Nullable String referenceColumnName) {
            Preconditions.checkArgument(columnName != null, "columnName must not be null");
            Preconditions.checkArgument(position != null, "position must not be null");
            boolean relativePosition =
                    position == ColumnPosition.BEFORE || position == ColumnPosition.AFTER;
            Preconditions.checkArgument(
                    relativePosition == (referenceColumnName != null),
                    relativePosition
                            ? "Column %s with position %s must define referenceColumnName"
                            : "Column %s with position %s must not define referenceColumnName",
                    columnName,
                    position);
            Preconditions.checkArgument(
                    !columnName.equals(referenceColumnName),
                    "Column %s cannot reference itself",
                    columnName);
            this.columnName = columnName;
            this.position = position;
            this.referenceColumnName = referenceColumnName;
        }

        public String getColumnName() {
            return columnName;
        }

        public ColumnPosition getPosition() {
            return position;
        }

        @Nullable
        public String getReferenceColumnName() {
            return referenceColumnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ColumnMove)) {
                return false;
            }
            ColumnMove that = (ColumnMove) o;
            return Objects.equals(columnName, that.columnName)
                    && position == that.position
                    && Objects.equals(referenceColumnName, that.referenceColumnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, position, referenceColumnName);
        }

        @Override
        public String toString() {
            return "ColumnMove{"
                    + "columnName='"
                    + columnName
                    + '\''
                    + ", position="
                    + position
                    + ", referenceColumnName='"
                    + referenceColumnName
                    + '\''
                    + '}';
        }
    }
}
