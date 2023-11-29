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

import java.util.List;
import java.util.Objects;

/**
 * A {@link SchemaChangeEvent} that represents an {@code DROP COLUMN} DDL, which may contain the
 * lenient column type changes.
 */
@PublicEvolving
public class DropColumnEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    private final List<Column> droppedColumns;

    public DropColumnEvent(TableId tableId, List<Column> droppedColumns) {
        this.tableId = tableId;
        this.droppedColumns = droppedColumns;
    }

    /** Returns the dropped columns. */
    public List<Column> getDroppedColumns() {
        return droppedColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DropColumnEvent)) {
            return false;
        }
        DropColumnEvent that = (DropColumnEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(droppedColumns, that.droppedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, droppedColumns);
    }

    @Override
    public String toString() {
        return "DropColumnEvent{"
                + "tableId="
                + tableId
                + ", droppedColumns="
                + droppedColumns
                + '}';
    }

    @Override
    public TableId tableId() {
        return tableId;
    }
}
