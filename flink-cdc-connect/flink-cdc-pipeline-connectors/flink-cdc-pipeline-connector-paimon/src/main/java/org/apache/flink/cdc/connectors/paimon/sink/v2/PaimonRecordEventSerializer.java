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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.paimon.sink.v2.blob.BlobWriteContext;
import org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketWrapperChangeEvent;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link PaimonRecordSerializer} for converting {@link Event} into {@link PaimonEvent} for {@link
 * PaimonWriter}.
 */
public class PaimonRecordEventSerializer implements PaimonRecordSerializer<Event> {

    // maintain the latest schema of tableId.
    private final Map<TableId, TableSchemaInfo> schemaMaps;

    // ZoneId for converting relevant type.
    private final ZoneId zoneId;

    public PaimonRecordEventSerializer(ZoneId zoneId) {
        schemaMaps = new HashMap<>();
        this.zoneId = zoneId;
    }

    /**
     * Update the BlobWriteContext for a specific table.
     *
     * <p>This is called by PaimonWriter when it has access to the Paimon table configuration. The
     * field getters will be recreated with the new BlobWriteContext to properly convert VARBINARY
     * fields to BLOB type.
     *
     * @param tableId The table identifier.
     * @param blobWriteContext The BlobWriteContext from the Paimon table.
     */
    public void updateBlobWriteContext(TableId tableId, BlobWriteContext blobWriteContext) {
        TableSchemaInfo schemaInfo = schemaMaps.get(tableId);
        if (schemaInfo != null) {
            schemaInfo.updateBlobWriteContext(blobWriteContext, zoneId);
        }
    }

    @Override
    public PaimonEvent serialize(Event event) {
        int bucket = 0;
        if (event instanceof BucketWrapperChangeEvent) {
            bucket = ((BucketWrapperChangeEvent) event).getBucket();
            event = ((BucketWrapperChangeEvent) event).getInnerEvent();
        }
        Identifier tableId = Identifier.fromString(((ChangeEvent) event).tableId().toString());
        if (event instanceof SchemaChangeEvent) {
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schemaMaps.put(
                        createTableEvent.tableId(),
                        new TableSchemaInfo(createTableEvent.getSchema(), zoneId));
            } else {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                Schema schema = schemaMaps.get(schemaChangeEvent.tableId()).getSchema();
                if (!SchemaUtils.isSchemaChangeEventRedundant(schema, schemaChangeEvent)) {
                    Schema newSchema =
                            SchemaUtils.applySchemaChangeEvent(schema, schemaChangeEvent);
                    // Preserve BlobWriteContext if it exists
                    BlobWriteContext existingContext =
                            schemaMaps.get(schemaChangeEvent.tableId()).getBlobWriteContext();
                    schemaMaps.put(
                            schemaChangeEvent.tableId(),
                            new TableSchemaInfo(newSchema, zoneId, existingContext));
                }
            }
            return new PaimonEvent(tableId, null, true);
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableSchemaInfo schemaInfo = schemaMaps.get(dataChangeEvent.tableId());
            List<GenericRow> genericRows =
                    PaimonWriterHelper.convertEventToFullGenericRows(
                            dataChangeEvent,
                            schemaInfo.getFieldGetters(),
                            schemaInfo.hasPrimaryKey());
            return new PaimonEvent(tableId, genericRows, false, bucket);
        } else {
            throw new IllegalArgumentException(
                    "failed to convert Input into PaimonEvent, unsupported event: " + event);
        }
    }

    public Map<TableId, TableSchemaInfo> getSchemaMaps() {
        return schemaMaps;
    }
}
