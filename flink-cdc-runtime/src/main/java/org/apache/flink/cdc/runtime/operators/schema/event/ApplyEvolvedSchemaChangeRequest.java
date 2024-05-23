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

package org.apache.flink.cdc.runtime.operators.schema.event;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

import java.util.List;
import java.util.Objects;

/**
 * The request from {@link SchemaOperator} to {@link SchemaRegistry} to request apply evolved schema
 * changes.
 */
public class ApplyEvolvedSchemaChangeRequest implements CoordinationRequest {

    private static final long serialVersionUID = 1L;

    /** The sender of the request. */
    private final TableId tableId;
    /** The schema changes. */
    private final List<SchemaChangeEvent> schemaChangeEvent;

    public ApplyEvolvedSchemaChangeRequest(
            TableId tableId, List<SchemaChangeEvent> schemaChangeEvent) {
        this.tableId = tableId;
        this.schemaChangeEvent = schemaChangeEvent;
    }

    public TableId getTableId() {
        return tableId;
    }

    public List<SchemaChangeEvent> getSchemaChangeEvent() {
        return schemaChangeEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ApplyEvolvedSchemaChangeRequest)) {
            return false;
        }
        ApplyEvolvedSchemaChangeRequest that = (ApplyEvolvedSchemaChangeRequest) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(schemaChangeEvent, that.schemaChangeEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schemaChangeEvent);
    }
}
