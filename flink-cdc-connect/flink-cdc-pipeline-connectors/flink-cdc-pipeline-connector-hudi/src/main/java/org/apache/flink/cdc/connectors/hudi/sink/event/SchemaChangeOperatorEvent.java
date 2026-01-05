/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink.event;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * An operator event that encapsulates a schema change and the resulting new schema.
 *
 * <p>This event is sent from the {@code MultiTableEventStreamWriteFunction} to the {@code
 * MultiTableStreamWriteOperatorCoordinator} to signal that a table's schema has changed in the CDC
 * stream. The coordinator uses this event to update its cached schema and recreate the write client
 * to ensure subsequent operations use the correct schema.
 */
public class SchemaChangeOperatorEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final Schema newSchema;

    /**
     * Constructs a new SchemaChangeOperatorEvent.
     *
     * @param tableId The ID of the table whose schema changed
     * @param newSchema The new schema after applying the schema change
     */
    public SchemaChangeOperatorEvent(TableId tableId, Schema newSchema) {
        this.tableId = tableId;
        this.newSchema = newSchema;
    }

    /**
     * Gets the ID of the table whose schema changed.
     *
     * @return The table ID
     */
    public TableId getTableId() {
        return tableId;
    }

    /**
     * Gets the new schema after the change.
     *
     * @return The new schema
     */
    public Schema getNewSchema() {
        return newSchema;
    }

    @Override
    public String toString() {
        return "SchemaChangeOperatorEvent{"
                + "tableId="
                + tableId
                + ", newSchema columns="
                + newSchema.getColumnCount()
                + '}';
    }
}
