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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.connectors.sqlserver.table.SqlServerReadableMetadata;
import org.apache.flink.cdc.connectors.sqlserver.utils.SqlServerSchemaUtils;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import io.debezium.data.Envelope;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** Event deserializer for {@link SqlServerDataSource}. */
@Internal
public class SqlServerEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;
    private final boolean includeSchemaChanges;
    private final List<SqlServerReadableMetadata> readableMetadataList;

    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    public SqlServerEventDeserializer(
            DebeziumChangelogMode changelogMode, boolean includeSchemaChanges) {
        super(new SqlServerSchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = new ArrayList<>();
    }

    public SqlServerEventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            List<SqlServerReadableMetadata> readableMetadataList) {
        super(new SqlServerSchemaDataTypeInference(), changelogMode);
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
                TableId tableId =
                        TableId.tableId(
                                dbzTableId.catalog(), dbzTableId.schema(), dbzTableId.table());
                switch (change.getType()) {
                    case CREATE:
                        Schema createSchema = SqlServerSchemaUtils.toSchema(change.getTable());
                        CreateTableEvent createTableEvent =
                                new CreateTableEvent(tableId, createSchema);
                        events.add(createTableEvent);
                        cache.put(dbzTableId, createTableEvent);
                        break;
                    case ALTER:
                        Schema newSchema = SqlServerSchemaUtils.toSchema(change.getTable());
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
                        // ignore others
                }
            }
            return events;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to deserialize SQL Server schema change event", e);
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
        // Using SourceRecordUtils keeps the namespace (database) in the TableId so that schema
        // change events and data change events refer to the same identifier.
        io.debezium.relational.TableId dbzTableId =
                org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId(record);
        return SqlServerSchemaUtils.toCdcTableId(dbzTableId);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        if (readableMetadataList == null || readableMetadataList.isEmpty()) {
            return metadataMap;
        }
        readableMetadataList.forEach(
                (sqlServerReadableMetadata -> {
                    Object metadata = sqlServerReadableMetadata.getConverter().read(record);
                    if (sqlServerReadableMetadata.equals(SqlServerReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                sqlServerReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(
                                sqlServerReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }
}
