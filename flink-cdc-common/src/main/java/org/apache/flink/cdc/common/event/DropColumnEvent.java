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

    private final List<String> droppedColumnNames;

    public DropColumnEvent(TableId tableId, List<String> droppedColumnNames) {
        this.tableId = tableId;
        this.droppedColumnNames = droppedColumnNames;
    }

    public List<String> getDroppedColumnNames() {
        return droppedColumnNames;
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
                && Objects.equals(droppedColumnNames, that.droppedColumnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, droppedColumnNames);
    }

    @Override
    public String toString() {
        return "DropColumnEvent{"
                + "tableId="
                + tableId
                + ", droppedColumnNames="
                + droppedColumnNames
                + '}';
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.DROP_COLUMN;
    }

    @Override
    public SchemaChangeEvent copy(TableId newTableId) {
        return new DropColumnEvent(newTableId, droppedColumnNames);
    }
}
