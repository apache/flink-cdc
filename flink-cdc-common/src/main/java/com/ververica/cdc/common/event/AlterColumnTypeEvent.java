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

import com.ververica.cdc.common.types.DataType;

import java.util.Map;
import java.util.Objects;

/**
 * A {@link SchemaChangeEvent} that represents an {@code ALTER COLUMN} DDL, which may contain the
 * lenient column type changes.
 */
public class AlterColumnTypeEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    /** key => column name, value => column type after changing. */
    private final Map<String, DataType> typeMapping;

    public AlterColumnTypeEvent(TableId tableId, Map<String, DataType> typeMapping) {
        this.tableId = tableId;
        this.typeMapping = typeMapping;
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
                && Objects.equals(typeMapping, that.typeMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, typeMapping);
    }

    @Override
    public String toString() {
        return "AlterColumnTypeEvent{"
                + "tableId="
                + tableId
                + ", nameMapping="
                + typeMapping
                + '}';
    }

    @Override
    public TableId tableId() {
        return tableId;
    }
}
