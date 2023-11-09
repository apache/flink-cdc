/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.event;

import org.apache.flink.annotation.PublicEvolving;

import com.ververica.cdc.common.schema.Column;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * A {@link SchemaChangeEvent} that represents an {@code ADD COLUMN} DDL, which may contain the
 * lenient column type changes.
 */
@PublicEvolving
public final class AddColumnEvent implements SchemaChangeEvent, Serializable {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    private final List<ColumnWithPosition> addedColumns;

    public AddColumnEvent(TableId tableId, List<ColumnWithPosition> addedColumns) {
        this.tableId = tableId;
        this.addedColumns = addedColumns;
    }

    /** Returns the added columns. */
    public List<ColumnWithPosition> getAddedColumns() {
        return addedColumns;
    }

    /** relative Position of column. */
    public enum ColumnPosition implements Serializable {
        BEFORE,
        AFTER,
        FIRST,
        LAST
    }

    /** represents result of an ADD COLUMN DDL that may change column sequence. */
    public static class ColumnWithPosition implements Serializable {

        final Column column;

        final ColumnPosition position;

        final Column otherColumn;

        /** In the default scenario, we add fields at the end of the column. */
        public ColumnWithPosition(Column column) {
            this.column = column;
            position = ColumnPosition.LAST;
            otherColumn = null;
        }

        public ColumnWithPosition(Column column, ColumnPosition position, Column otherColumn) {
            this.column = column;
            this.position = position;
            this.otherColumn = otherColumn;
        }

        public Column getColumn() {
            return column;
        }

        public ColumnPosition getPosition() {
            return position;
        }

        public Column getOtherColumn() {
            return otherColumn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ColumnWithPosition that = (ColumnWithPosition) o;
            return Objects.equals(column, that.column)
                    && Objects.equals(position, that.position)
                    && Objects.equals(otherColumn, that.otherColumn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column, position, otherColumn);
        }

        @Override
        public String toString() {
            return "ColumnWithPosition{"
                    + "column="
                    + column
                    + ", position="
                    + position
                    + ", otherColumn="
                    + otherColumn
                    + '}';
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
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
}
