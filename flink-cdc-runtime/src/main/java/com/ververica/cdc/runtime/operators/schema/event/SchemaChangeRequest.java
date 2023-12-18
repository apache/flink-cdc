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

package com.ververica.cdc.runtime.operators.schema.event;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.runtime.operators.schema.SchemaOperator;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;

import java.util.Objects;

/**
 * The request from {@link SchemaOperator} to {@link SchemaRegistry} to request to change schemas.
 */
public class SchemaChangeRequest implements CoordinationRequest {

    private static final long serialVersionUID = 1L;

    /** The sender of the request. */
    private final TableId tableId;
    /** The schema changes. */
    private final SchemaChangeEvent schemaChangeEvent;

    public SchemaChangeRequest(TableId tableId, SchemaChangeEvent schemaChangeEvent) {
        this.tableId = tableId;
        this.schemaChangeEvent = schemaChangeEvent;
    }

    public TableId getTableId() {
        return tableId;
    }

    public SchemaChangeEvent getSchemaChangeEvent() {
        return schemaChangeEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaChangeRequest)) {
            return false;
        }
        SchemaChangeRequest that = (SchemaChangeRequest) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(schemaChangeEvent, that.schemaChangeEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schemaChangeEvent);
    }
}
