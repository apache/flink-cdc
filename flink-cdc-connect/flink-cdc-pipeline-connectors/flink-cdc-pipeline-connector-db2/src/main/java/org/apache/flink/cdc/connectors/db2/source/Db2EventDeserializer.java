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

package org.apache.flink.cdc.connectors.db2.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.connectors.db2.table.Db2ReadableMetaData;
import org.apache.flink.cdc.connectors.db2.utils.Db2SchemaUtils;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import io.debezium.data.Envelope;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** Event deserializer for {@link Db2DataSource}. */
@Internal
public class Db2EventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(Db2EventDeserializer.class);

    private final boolean includeSchemaChanges;
    private final List<Db2ReadableMetaData> readableMetadataList;

    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    public Db2EventDeserializer(DebeziumChangelogMode changelogMode, boolean includeSchemaChanges) {
        super(new Db2SchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = new ArrayList<>();
    }

    public Db2EventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            List<Db2ReadableMetaData> readableMetadataList) {
        super(new Db2SchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        if (!includeSchemaChanges) {
            return Collections.emptyList();
        }

        try {
            TableChanges changes =
                    TABLE_CHANGE_SERIALIZER.deserialize(
                            getHistoryRecord(record)
                                    .document()
                                    .getArray(
                                            io.debezium.relational.history.HistoryRecord.Fields
                                                    .TABLE_CHANGES),
                            true);

            final Map<io.debezium.relational.TableId, CreateTableEvent> cache =
                    getCreateTableEventCache();
            List<SchemaChangeEvent> events = new ArrayList<>();
            for (TableChange change : changes) {
                io.debezium.relational.TableId dbzTableId = change.getId();
                TableId tableId = Db2SchemaUtils.toCdcTableId(dbzTableId);
                switch (change.getType()) {
                    case CREATE:
                        Schema createSchema = Db2SchemaUtils.toSchema(change.getTable());
                        CreateTableEvent createTableEvent =
                                new CreateTableEvent(tableId, createSchema);
                        events.add(createTableEvent);
                        cache.put(dbzTableId, createTableEvent);
                        break;
                    case ALTER:
                        Schema newSchema = Db2SchemaUtils.toSchema(change.getTable());
                        CreateTableEvent oldCreateTableEvent = cache.get(dbzTableId);
                        CreateTableEvent newCreateTableEvent =
                                new CreateTableEvent(tableId, newSchema);
                        if (oldCreateTableEvent == null) {
                            events.add(newCreateTableEvent);
                        } else {
                            events.addAll(
                                    SchemaMergingUtils.getSchemaDifference(
                                            tableId, oldCreateTableEvent.getSchema(), newSchema));
                        }
                        cache.put(dbzTableId, newCreateTableEvent);
                        break;
                    case DROP:
                        events.add(new DropTableEvent(tableId));
                        cache.remove(dbzTableId);
                        break;
                    default:
                        LOG.warn(
                                "Ignored unsupported schema change type '{}' for table '{}'.",
                                change.getType(),
                                tableId);
                }
            }
            return events;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize Db2 schema change event", e);
        }
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        org.apache.kafka.connect.data.Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        return isSchemaChangeEvent(record);
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        // Debezium source record contains database/schema/table information in the source struct.
        // The namespace (database) is dropped here to keep consistent with the "schema.table"
        // format used by the "tables" option.
        io.debezium.relational.TableId dbzTableId =
                org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId(record);
        return Db2SchemaUtils.toCdcTableId(dbzTableId);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        if (readableMetadataList == null || readableMetadataList.isEmpty()) {
            return metadataMap;
        }
        readableMetadataList.forEach(
                (db2ReadableMetaData -> {
                    Object metadata = db2ReadableMetaData.getConverter().read(record);
                    if (db2ReadableMetaData.equals(Db2ReadableMetaData.OP_TS)) {
                        metadataMap.put(
                                db2ReadableMetaData.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(db2ReadableMetaData.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }
}
