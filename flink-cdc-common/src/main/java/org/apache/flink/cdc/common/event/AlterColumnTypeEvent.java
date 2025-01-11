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
import org.apache.flink.cdc.common.types.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link SchemaChangeEvent} that represents an {@code ALTER COLUMN} DDL, which may contain the
 * lenient column type changes.
 */
@PublicEvolving
public class AlterColumnTypeEvent implements SchemaChangeEventWithPreSchema, SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    /** key => column name, value => column type after changing. */
    private final Map<String, DataType> typeMapping;

    private final Map<String, DataType> oldTypeMapping;

    public AlterColumnTypeEvent(TableId tableId, Map<String, DataType> typeMapping) {
        this.tableId = tableId;
        this.typeMapping = typeMapping;
        this.oldTypeMapping = new HashMap<>();
    }

    public AlterColumnTypeEvent(
            TableId tableId,
            Map<String, DataType> typeMapping,
            Map<String, DataType> oldTypeMapping) {
        this.tableId = tableId;
        this.typeMapping = typeMapping;
        this.oldTypeMapping = oldTypeMapping;
    }

    /** Returns the type mapping. */
    public Map<String, DataType> getTypeMapping() {
        return typeMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlterColumnTypeEvent)) {
            return false;
        }
        AlterColumnTypeEvent that = (AlterColumnTypeEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(typeMapping, that.typeMapping)
                && Objects.equals(oldTypeMapping, that.oldTypeMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, typeMapping, oldTypeMapping);
    }

    @Override
    public String toString() {
        if (hasPreSchema()) {
            return "AlterColumnTypeEvent{"
                    + "tableId="
                    + tableId
                    + ", typeMapping="
                    + typeMapping
                    + ", oldTypeMapping="
                    + oldTypeMapping
                    + '}';
        } else {
            return "AlterColumnTypeEvent{"
                    + "tableId="
                    + tableId
                    + ", typeMapping="
                    + typeMapping
                    + '}';
        }
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    public Map<String, DataType> getOldTypeMapping() {
        return oldTypeMapping;
    }

    @Override
    public boolean hasPreSchema() {
        return !oldTypeMapping.isEmpty();
    }

    @Override
    public void fillPreSchema(Schema oldTypeSchema) {
        oldTypeMapping.clear();
        oldTypeMapping.putAll(
                oldTypeSchema.getColumns().stream()
                        .filter(e -> typeMapping.containsKey(e.getName()) && e.getType() != null)
                        .collect(Collectors.toMap(Column::getName, Column::getType)));
    }

    @Override
    public boolean trimRedundantChanges() {
        if (hasPreSchema()) {
            Set<String> redundantlyChangedColumns =
                    typeMapping.keySet().stream()
                            .filter(e -> Objects.equals(typeMapping.get(e), oldTypeMapping.get(e)))
                            .collect(Collectors.toSet());

            // Remove redundant alter column type records that doesn't really change the type
            typeMapping.keySet().removeAll(redundantlyChangedColumns);
            oldTypeMapping.keySet().removeAll(redundantlyChangedColumns);
        }
        return !typeMapping.isEmpty();
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.ALTER_COLUMN_TYPE;
    }

    @Override
    public SchemaChangeEvent copy(TableId newTableId) {
        return new AlterColumnTypeEvent(newTableId, typeMapping, oldTypeMapping);
    }
}
