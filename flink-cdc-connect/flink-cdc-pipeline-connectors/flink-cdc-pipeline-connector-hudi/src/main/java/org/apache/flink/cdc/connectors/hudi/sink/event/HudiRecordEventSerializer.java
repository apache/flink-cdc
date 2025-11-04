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

package org.apache.flink.cdc.connectors.hudi.sink.event;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.hudi.sink.util.RowDataUtils;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link HudiRecordSerializer} for converting {@link Event} into {@link HoodieFlinkInternalRow}
 * for Hudi writing.
 *
 * <p>This serializer maintains schema state per table and handles multi-table CDC events by:
 *
 * <ul>
 *   <li>Caching schemas from CreateTableEvent and SchemaChangeEvent
 *   <li>Converting DataChangeEvent to HoodieFlinkInternalRow using cached schemas
 *   <li>Supporting bucket-wrapped events from upstream operators
 * </ul>
 *
 * <p>Assumes that CreateTableEvent will always arrive before DataChangeEvent for each table,
 * following the standard CDC pipeline startup sequence.
 */
public class HudiRecordEventSerializer implements HudiRecordSerializer<Event> {

    /** Schema cache per table - populated from CreateTableEvent and SchemaChangeEvent. */
    private final Map<TableId, Schema> schemaMaps;

    /** Field getter cache per table for efficient conversion. */
    private final Map<TableId, List<RecordData.FieldGetter>> fieldGetterCache;

    /** Zone ID for timestamp conversion. */
    private final ZoneId zoneId;

    public HudiRecordEventSerializer(ZoneId zoneId) {
        this.schemaMaps = new HashMap<>();
        this.fieldGetterCache = new HashMap<>();
        this.zoneId = zoneId;
    }

    /**
     * Serialize an Event into HoodieFlinkInternalRow.
     *
     * @param event The input event (can be BucketWrappedChangeEvent)
     * @param fileId The file ID to assign to the record
     * @param instantTime The instant time to assign to the record
     * @return HoodieFlinkInternalRow or null for schema events
     * @throws IllegalArgumentException if event type is unsupported
     * @throws IllegalStateException if schema is not available for DataChangeEvent
     */
    @Override
    public HoodieFlinkInternalRow serialize(Event event, String fileId, String instantTime) {
        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            schemaMaps.put(createTableEvent.tableId(), createTableEvent.getSchema());
            // Clear field getter cache for this table since schema changed
            fieldGetterCache.remove(createTableEvent.tableId());
            // Schema events don't produce records
            return null;

        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            Schema existingSchema = schemaMaps.get(schemaChangeEvent.tableId());
            if (existingSchema != null
                    && !SchemaUtils.isSchemaChangeEventRedundant(
                            existingSchema, schemaChangeEvent)) {
                Schema newSchema =
                        SchemaUtils.applySchemaChangeEvent(existingSchema, schemaChangeEvent);
                schemaMaps.put(schemaChangeEvent.tableId(), newSchema);
                // Clear field getter cache for this table since schema changed
                fieldGetterCache.remove(schemaChangeEvent.tableId());
            }
            // Schema events don't produce records
            return null;

        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            Schema schema = schemaMaps.get(dataChangeEvent.tableId());

            if (schema == null) {
                throw new IllegalStateException(
                        "No schema available for table "
                                + dataChangeEvent.tableId()
                                + ". CreateTableEvent should arrive before DataChangeEvent.");
            }

            // Convert DataChangeEvent to HoodieFlinkInternalRow using utility function
            return RowDataUtils.convertDataChangeEventToHoodieFlinkInternalRow(
                    dataChangeEvent, schema, zoneId, fileId, instantTime);

        } else {
            throw new IllegalArgumentException(
                    "Unsupported event type for Hudi serialization: "
                            + event.getClass().getSimpleName());
        }
    }

    /**
     * Serialize an Event into HoodieFlinkInternalRow without fileId and instantTime. The fileId and
     * instantTime will be set later by the caller.
     *
     * @param event The input event (can be BucketWrappedChangeEvent)
     * @return HoodieFlinkInternalRow or null for schema events
     * @throws IllegalArgumentException if event type is unsupported
     * @throws IllegalStateException if schema is not available for DataChangeEvent
     */
    @Override
    public HoodieFlinkInternalRow serialize(Event event) {
        return serialize(event, "temp", "temp");
    }

    /**
     * Get cached schema for a table.
     *
     * @param tableId The table identifier
     * @return Schema or null if not cached
     */
    public Schema getSchema(TableId tableId) {
        return schemaMaps.get(tableId);
    }

    /**
     * Check if schema is cached for a table.
     *
     * @param tableId The table identifier
     * @return true if schema is cached
     */
    public boolean hasSchema(TableId tableId) {
        return schemaMaps.containsKey(tableId);
    }

    /**
     * Set schema for a table. Used to initialize table-specific serializers with schema.
     *
     * @param tableId The table identifier
     * @param schema The schema to set
     */
    public void setSchema(TableId tableId, Schema schema) {
        schemaMaps.put(tableId, schema);
        // Clear cached field getters for this table so they get recreated with the new schema
        fieldGetterCache.remove(tableId);
    }
}
