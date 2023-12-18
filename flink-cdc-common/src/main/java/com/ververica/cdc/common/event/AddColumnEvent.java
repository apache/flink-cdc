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

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.schema.Column;

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

        /** The added column. */
        private final Column addColumn;

        /** The position of the added column. */
        private final ColumnPosition position;

        /** The added column lies in the position relative to this column. */
        private final @Nullable Column existingColumn;

        /** In the default scenario, we add fields at the end of the column. */
        public ColumnWithPosition(Column addColumn) {
            this.addColumn = addColumn;
            position = ColumnPosition.LAST;
            existingColumn = null;
        }

        public ColumnWithPosition(
                Column addColumn, ColumnPosition position, Column existingColumn) {
            this.addColumn = addColumn;
            this.position = position;
            this.existingColumn = existingColumn;
        }

        public Column getAddColumn() {
            return addColumn;
        }

        public ColumnPosition getPosition() {
            return position;
        }

        public Column getExistingColumn() {
            return existingColumn;
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
                    && Objects.equals(existingColumn, position1.existingColumn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(addColumn, position, existingColumn);
        }

        @Override
        public String toString() {
            return "ColumnWithPosition{"
                    + "column="
                    + addColumn
                    + ", position="
                    + position
                    + ", existingColumn="
                    + existingColumn
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
}
