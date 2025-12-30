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
import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link SchemaChangeEvent} that represents column position changes in a table, such as {@code
 * ALTER TABLE ... MODIFY COLUMN ... AFTER/BEFORE} DDL operations.
 */
@PublicEvolving
public class AlterColumnPositionEvent implements SchemaChangeEventWithPreSchema, SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    /** key => column name, value => new position information. */
    private final Map<String, ColumnPosition> positionMapping;

    /** key => column name, value => old position (0-based). */
    private final Map<String, Integer> oldPositionMapping;

    public AlterColumnPositionEvent(TableId tableId, Map<String, ColumnPosition> positionMapping) {
        this.tableId = tableId;
        this.positionMapping = positionMapping;
        this.oldPositionMapping = new HashMap<>();
    }

    public AlterColumnPositionEvent(
            TableId tableId,
            Map<String, ColumnPosition> positionMapping,
            Map<String, Integer> oldPositionMapping) {
        this.tableId = tableId;
        this.positionMapping = positionMapping;
        this.oldPositionMapping = oldPositionMapping;
    }

    /** Returns the position mapping. */
    public Map<String, ColumnPosition> getPositionMapping() {
        return positionMapping;
    }

    /** Returns the old position mapping. */
    public Map<String, Integer> getOldPositionMapping() {
        return oldPositionMapping;
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.ALTER_COLUMN_POSITION;
    }

    @Override
    public SchemaChangeEvent copy(TableId newTableId) {
        return new AlterColumnPositionEvent(newTableId, positionMapping, oldPositionMapping);
    }

    @Override
    public boolean hasPreSchema() {
        return !oldPositionMapping.isEmpty();
    }

    @Override
    public void fillPreSchema(Schema oldSchema) {
        oldPositionMapping.clear();
        List<Column> columns = oldSchema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getName();
            if (positionMapping.containsKey(columnName)) {
                oldPositionMapping.put(columnName, i);
            }
        }
    }

    @Override
    public boolean trimRedundantChanges() {
        if (hasPreSchema()) {
            Set<String> redundantChanges =
                    positionMapping.entrySet().stream()
                            .filter(
                                    entry -> {
                                        String columnName = entry.getKey();
                                        ColumnPosition newPos = entry.getValue();
                                        Integer oldPos = oldPositionMapping.get(columnName);
                                        return oldPos != null
                                                && oldPos.equals(newPos.getAbsolutePosition());
                                    })
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());

            positionMapping.keySet().removeAll(redundantChanges);
            oldPositionMapping.keySet().removeAll(redundantChanges);
        }
        return !positionMapping.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlterColumnPositionEvent)) {
            return false;
        }
        AlterColumnPositionEvent that = (AlterColumnPositionEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(positionMapping, that.positionMapping)
                && Objects.equals(oldPositionMapping, that.oldPositionMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, positionMapping, oldPositionMapping);
    }

    @Override
    public String toString() {
        if (hasPreSchema()) {
            return "AlterColumnPositionEvent{"
                    + "tableId="
                    + tableId
                    + ", positionMapping="
                    + positionMapping
                    + ", oldPositionMapping="
                    + oldPositionMapping
                    + '}';
        } else {
            return "AlterColumnPositionEvent{"
                    + "tableId="
                    + tableId
                    + ", positionMapping="
                    + positionMapping
                    + '}';
        }
    }

    /** Represents the position information for a column. */
    public static class ColumnPosition implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int absolutePosition; // 0-based absolute position
        private final @Nullable String afterColumn; // relative position: after this column
        private final boolean isFirst; // whether to move to first position

        /** Create position for first column. */
        public static ColumnPosition first() {
            return new ColumnPosition(0, null, true);
        }

        /** Create position after specified column. */
        public static ColumnPosition after(String columnName) {
            return new ColumnPosition(-1, columnName, false);
        }

        /** Create position at absolute index. */
        public static ColumnPosition at(int position) {
            return new ColumnPosition(position, null, false);
        }

        private ColumnPosition(
                int absolutePosition, @Nullable String afterColumn, boolean isFirst) {
            this.absolutePosition = absolutePosition;
            this.afterColumn = afterColumn;
            this.isFirst = isFirst;
        }

        public int getAbsolutePosition() {
            return absolutePosition;
        }

        @Nullable
        public String getAfterColumn() {
            return afterColumn;
        }

        public boolean isFirst() {
            return isFirst;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof ColumnPosition)) {
                return false;
            }

            ColumnPosition that = (ColumnPosition) o;
            return absolutePosition == that.absolutePosition
                    && isFirst == that.isFirst
                    && Objects.equals(afterColumn, that.afterColumn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(absolutePosition, afterColumn, isFirst);
        }

        @Override
        public String toString() {
            return "ColumnPosition{"
                    + "absolutePosition="
                    + absolutePosition
                    + ", afterColumn='"
                    + afterColumn
                    + '\''
                    + ", isFirst="
                    + isFirst
                    + '}';
        }
    }
}
