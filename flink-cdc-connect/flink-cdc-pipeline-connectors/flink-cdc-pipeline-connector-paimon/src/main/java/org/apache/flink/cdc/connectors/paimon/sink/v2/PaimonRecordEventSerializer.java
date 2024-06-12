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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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

    @Override
    public PaimonEvent serialize(Event event) {
        Identifier tableId =
                Identifier.create(
                        ((ChangeEvent) event).tableId().getSchemaName(),
                        ((ChangeEvent) event).tableId().getTableName());
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            Tuple2<TableId, Schema> appliedSchema =
                    SchemaUtils.applySchemaChangeEvent(
                            schemaChangeEvent,
                            Optional.ofNullable(
                                            schemaMaps.getOrDefault(
                                                    schemaChangeEvent.tableId(), null))
                                    .map(TableSchemaInfo::getSchema)
                                    .orElse(null));
            if (appliedSchema.f1 != null) {
                schemaMaps.put(appliedSchema.f0, new TableSchemaInfo(appliedSchema.f1, zoneId));
            } else {
                schemaMaps.remove(appliedSchema.f0, null);
            }
            return new PaimonEvent(tableId, null, true);
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            GenericRow genericRow =
                    PaimonWriterHelper.convertEventToGenericRow(
                            dataChangeEvent,
                            schemaMaps.get(dataChangeEvent.tableId()).getFieldGetters());
            return new PaimonEvent(tableId, genericRow);
        } else {
            throw new IllegalArgumentException(
                    "failed to convert Input into PaimonEvent, unsupported event: " + event);
        }
    }
}
