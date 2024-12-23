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
        return new ColumnWithPosition(addColumn, ColumnPosition.FIRST, null);
    }

    public static AddColumnEvent.ColumnWithPosition last(Column addColumn) {
        return new ColumnWithPosition(addColumn, ColumnPosition.LAST, null);
    }

    public static AddColumnEvent.ColumnWithPosition before(
            Column addColumn, String existedColumnName) {
        return new ColumnWithPosition(addColumn, ColumnPosition.BEFORE, existedColumnName);
    }

    public static AddColumnEvent.ColumnWithPosition after(
            Column addColumn, String existedColumnName) {
        return new ColumnWithPosition(addColumn, ColumnPosition.AFTER, existedColumnName);
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
        private final @Nullable String existedColumnName;

        /** In the default scenario, we add fields at the end of the column. */
        public ColumnWithPosition(Column addColumn) {
            this.addColumn = addColumn;
            position = ColumnPosition.LAST;
            existedColumnName = null;
        }

        public ColumnWithPosition(
                Column addColumn, ColumnPosition position, @Nullable String existedColumnName) {
            this.addColumn = addColumn;
            this.position = position;
            this.existedColumnName = existedColumnName;
        }

        public Column getAddColumn() {
            return addColumn;
        }

        public ColumnPosition getPosition() {
            return position;
        }

        @Nullable
        public String getExistedColumnName() {
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
