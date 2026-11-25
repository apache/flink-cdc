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
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * A {@link SchemaChangeEvent} that represents an {@code ADD COLUMN} DDL, which may contain the
 * lenient column type changes.
 */
@PublicEvolving
public final class AddColumnEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    private final List<ColumnWithPosition> addedColumns;

    public AddColumnEvent(TableId tableId, List<ColumnWithPosition> addedColumns) {
        this.tableId = tableId;
        this.addedColumns = addedColumns;
    }

    public static AddColumnEvent.ColumnWithPosition first(Column addColumn) {
        return ColumnWithPosition.of(
                addColumn, org.apache.flink.cdc.common.event.ColumnPosition.FIRST, null);
    }

    public static AddColumnEvent.ColumnWithPosition last(Column addColumn) {
        return ColumnWithPosition.of(
                addColumn, org.apache.flink.cdc.common.event.ColumnPosition.LAST, null);
    }

    public static AddColumnEvent.ColumnWithPosition before(
            Column addColumn, String referenceColumnName) {
        return ColumnWithPosition.of(
                addColumn,
                org.apache.flink.cdc.common.event.ColumnPosition.BEFORE,
                referenceColumnName);
    }

    public static AddColumnEvent.ColumnWithPosition after(
            Column addColumn, String referenceColumnName) {
        return ColumnWithPosition.of(
                addColumn,
                org.apache.flink.cdc.common.event.ColumnPosition.AFTER,
                referenceColumnName);
    }

    /** Returns the added columns. */
    public List<ColumnWithPosition> getAddedColumns() {
        return addedColumns;
    }

    /**
     * Relative position of a column.
     *
     * @deprecated Use {@link org.apache.flink.cdc.common.event.ColumnPosition} instead.
     */
    @Deprecated
    public enum ColumnPosition implements Serializable {
        BEFORE,
        AFTER,
        FIRST,
        LAST;

        /** Converts this legacy position to the top-level position type. */
        public org.apache.flink.cdc.common.event.ColumnPosition toColumnPosition() {
            return org.apache.flink.cdc.common.event.ColumnPosition.valueOf(name());
        }

        /** Converts a top-level position to the legacy position type, preserving {@code null}. */
        @Nullable
        public static ColumnPosition fromColumnPosition(
                @Nullable org.apache.flink.cdc.common.event.ColumnPosition position) {
            return position == null ? null : ColumnPosition.valueOf(position.name());
        }
    }

    /** represents result of an ADD COLUMN DDL that may change column sequence. */
    public static class ColumnWithPosition implements Serializable {

        private static final long serialVersionUID = 487237014867351187L;

        /** The added column. */
        private final Column addColumn;

        /** The position of the added column. */
        private final ColumnPosition position;

        /** The added column lies in the position relative to this column. */
        private final @Nullable String existedColumnName;

        /** In the default scenario, we add fields at the end of the column. */
        public ColumnWithPosition(Column addColumn) {
            this.addColumn = addColumn;
            position = ColumnPosition.LAST;
            existedColumnName = null;
        }

        /**
         * @deprecated Use {@link #of(Column, org.apache.flink.cdc.common.event.ColumnPosition,
         *     String)} instead.
         */
        @Deprecated
        public ColumnWithPosition(
                Column addColumn, ColumnPosition position, @Nullable String existedColumnName) {
            this.addColumn = addColumn;
            this.position = position;
            this.existedColumnName = existedColumnName;
        }

        public static ColumnWithPosition of(
                Column addColumn,
                org.apache.flink.cdc.common.event.ColumnPosition position,
                @Nullable String referenceColumnName) {
            Preconditions.checkArgument(addColumn != null, "addColumn must not be null");
            Preconditions.checkArgument(position != null, "position must not be null");
            boolean relativePosition =
                    position == org.apache.flink.cdc.common.event.ColumnPosition.BEFORE
                            || position == org.apache.flink.cdc.common.event.ColumnPosition.AFTER;
            Preconditions.checkArgument(
                    relativePosition == (referenceColumnName != null),
                    relativePosition
                            ? "Column %s with position %s must define referenceColumnName"
                            : "Column %s with position %s must not define referenceColumnName",
                    addColumn.getName(),
                    position);
            Preconditions.checkArgument(
                    !addColumn.getName().equals(referenceColumnName),
                    "Added column %s cannot reference itself",
                    addColumn.getName());
            return new ColumnWithPosition(
                    addColumn, ColumnPosition.fromColumnPosition(position), referenceColumnName);
        }

        public Column getAddColumn() {
            return addColumn;
        }

        /**
         * @deprecated Use {@link #getColumnPosition()} instead.
         */
        @Deprecated
        public ColumnPosition getPosition() {
            return position;
        }

        public org.apache.flink.cdc.common.event.ColumnPosition getColumnPosition() {
            return position.toColumnPosition();
        }

        /**
         * @deprecated Use {@link #getReferenceColumnName()} instead.
         */
        @Deprecated
        @Nullable
        public String getExistedColumnName() {
            return existedColumnName;
        }

        @Nullable
        public String getReferenceColumnName() {
            return existedColumnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ColumnWithPosition)) {
                return false;
            }
            ColumnWithPosition position1 = (ColumnWithPosition) o;
            return Objects.equals(addColumn, position1.addColumn)
                    && position == position1.position
                    && Objects.equals(existedColumnName, position1.existedColumnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(addColumn, position, existedColumnName);
        }

        @Override
        public String toString() {
            return "ColumnWithPosition{"
                    + "column="
                    + addColumn
                    + ", position="
                    + position
                    + ", existedColumnName="
                    + existedColumnName
                    + '}';
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AddColumnEvent)) {
            return false;
        }
        AddColumnEvent that = (AddColumnEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(addedColumns, that.addedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, addedColumns);
    }

    @Override
    public String toString() {
        return "AddColumnEvent{" + "tableId=" + tableId + ", addedColumns=" + addedColumns + '}';
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.ADD_COLUMN;
    }

    @Override
    public SchemaChangeEvent copy(TableId newTableId) {
        return new AddColumnEvent(newTableId, addedColumns);
    }
}
