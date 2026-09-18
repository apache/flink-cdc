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
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.connectors.db2.table.Db2ReadableMetadata;
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
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
    private final boolean includeSchemaChanges;
    private final List<Db2ReadableMetadata> readableMetadataList;

    /**
     * Cache to compute schema differences for ALTER events.
     *
     * <p>This cache is runtime-only and will be reconstructed from checkpointed split state (see
     * {@link #initializeTableSchemaCacheFromSplitSchemas(Map)}). It must not be {@code final}
     * because Java deserialization bypasses field initializers for {@code transient} fields.
     */
    private transient Map<TableId, Schema> tableSchemaCache;

    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    public Db2EventDeserializer(DebeziumChangelogMode changelogMode, boolean includeSchemaChanges) {
        super(new Db2SchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = new ArrayList<>();
        this.tableSchemaCache = new HashMap<>();
    }

    public Db2EventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            List<Db2ReadableMetadata> readableMetadataList) {
        super(new Db2SchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = readableMetadataList;
        this.tableSchemaCache = new HashMap<>();
    }

    /**
     * Initializes the schema cache from checkpointed split state.
     *
     * <p>The incremental source checkpoints Debezium {@link TableChange}s in {@code StreamSplit}'s
     * {@code tableSchemas}. We use it as the source of truth to (re)build the local {@link Schema}
     * cache after failover or task redistribution.
     */
    public void initializeTableSchemaCacheFromSplitSchemas(
            Map<io.debezium.relational.TableId, TableChange> tableSchemas) {
        if (!includeSchemaChanges || tableSchemas == null || tableSchemas.isEmpty()) {
            return;
        }
        final Map<TableId, Schema> cache = getTableSchemaCache();
        for (Map.Entry<io.debezium.relational.TableId, TableChange> entry :
                tableSchemas.entrySet()) {
            final io.debezium.relational.TableId dbzTableId = entry.getKey();
            final TableChange tableChange = entry.getValue();
            if (dbzTableId == null || tableChange == null || tableChange.getTable() == null) {
                continue;
            }
            final TableId tableId =
                    TableId.tableId(dbzTableId.catalog(), dbzTableId.schema(), dbzTableId.table());
            cache.putIfAbsent(tableId, Db2SchemaUtils.toSchema(tableChange.getTable()));
        }
    }

    private Map<TableId, Schema> getTableSchemaCache() {
        if (tableSchemaCache == null) {
            tableSchemaCache = new HashMap<>();
        }
        return tableSchemaCache;
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

            final Map<TableId, Schema> cache = getTableSchemaCache();
            List<SchemaChangeEvent> events = new ArrayList<>();
            for (TableChange change : changes) {
                TableId tableId =
                        TableId.tableId(
                                change.getId().catalog(),
                                change.getId().schema(),
                                change.getId().table());
                switch (change.getType()) {
                    case CREATE:
                        Schema createdSchema = Db2SchemaUtils.toSchema(change.getTable());
                        events.add(
                                new org.apache.flink.cdc.common.event.CreateTableEvent(
                                        tableId, createdSchema));
                        cache.put(tableId, createdSchema);
                        break;
                    case ALTER:
                        Schema newSchema = Db2SchemaUtils.toSchema(change.getTable());
                        Schema oldSchema = cache.get(tableId);
                        if (oldSchema == null) {
                            events.add(
                                    new org.apache.flink.cdc.common.event.CreateTableEvent(
                                            tableId, newSchema));
                        } else {
                            events.addAll(
                                    SchemaMergingUtils.getSchemaDifference(
                                            tableId, oldSchema, newSchema));
                        }
                        cache.put(tableId, newSchema);
                        break;
                    case DROP:
                        events.add(new org.apache.flink.cdc.common.event.DropTableEvent(tableId));
                        cache.remove(tableId);
                        break;
                    default:
                        // ignore others
                }
            }
            return events;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize DB2 schema change event", e);
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
        return Db2SchemaUtils.toCdcTableId(dbzTableId);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        if (readableMetadataList == null || readableMetadataList.isEmpty()) {
            return metadataMap;
        }
        readableMetadataList.forEach(
                (db2ReadableMetadata -> {
                    Object metadata = db2ReadableMetadata.getConverter().read(record);
                    if (db2ReadableMetadata.equals(Db2ReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                db2ReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(db2ReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }

    @Override
    protected Object convertToDecimal(
            DecimalType decimalType, Object dbzObj, org.apache.kafka.connect.data.Schema schema) {
        BigDecimal db2Decimal = decodeDb2CdcDecimal(dbzObj);
        if (db2Decimal != null) {
            return DecimalData.fromBigDecimal(
                    db2Decimal, decimalType.getPrecision(), decimalType.getScale());
        }
        return super.convertToDecimal(decimalType, dbzObj, schema);
    }

    private static BigDecimal decodeDb2CdcDecimal(Object dbzObj) {
        byte[] bytes;
        if (dbzObj instanceof byte[]) {
            bytes = (byte[]) dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer duplicate = ((ByteBuffer) dbzObj).duplicate();
            bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
        } else if (dbzObj instanceof String) {
            try {
                bytes = new BigDecimal((String) dbzObj).unscaledValue().toByteArray();
            } catch (NumberFormatException e) {
                return null;
            }
        } else if (dbzObj instanceof BigDecimal) {
            bytes = ((BigDecimal) dbzObj).unscaledValue().toByteArray();
        } else if (dbzObj instanceof BigInteger) {
            bytes = ((BigInteger) dbzObj).toByteArray();
        } else {
            return null;
        }

        StringBuilder decimalText = new StringBuilder(bytes.length);
        for (int i = bytes.length - 1; i >= 0; i--) {
            int ch = bytes[i] & 0xFF;
            if (ch == 0 || (ch & 0x80) != 0) {
                continue;
            }
            if ((ch >= '0' && ch <= '9') || ch == '.' || ch == '-' || ch == '+') {
                decimalText.append((char) ch);
                continue;
            }
            return null;
        }
        if (decimalText.length() == 0) {
            return null;
        }
        try {
            return new BigDecimal(decimalText.toString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}
