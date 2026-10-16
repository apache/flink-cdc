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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.mysql.source.parser.CustomMySqlAntlrDdlParser;
import org.apache.flink.cdc.connectors.mysql.table.MySqlReadableMetadata;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getHistoryRecord;

/** Event deserializer for {@link MySqlDataSource}. */
@Internal
public class MySqlEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MySqlEventDeserializer.class);

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final boolean includeSchemaChanges;
    private final boolean tinyInt1isBit;
    private final boolean includeComments;

    private transient Tables tables;
    private transient CustomMySqlAntlrDdlParser customParser;

    private final List<MySqlReadableMetadata> readableMetadataList;
    private final boolean isTableIdCaseInsensitive;
    private transient ConcurrentMap<TableId, DataType> dataTypeCache;

    public MySqlEventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            boolean tinyInt1isBit,
            boolean isTableIdCaseInsensitive) {
        this(
                changelogMode,
                includeSchemaChanges,
                new ArrayList<>(),
                includeSchemaChanges,
                tinyInt1isBit,
                isTableIdCaseInsensitive);
    }

    public MySqlEventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            List<MySqlReadableMetadata> readableMetadataList,
            boolean includeComments,
            boolean tinyInt1isBit,
            boolean isTableIdCaseInsensitive) {
        super(new MySqlSchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = readableMetadataList;
        this.includeComments = includeComments;
        this.tinyInt1isBit = tinyInt1isBit;
        this.isTableIdCaseInsensitive = isTableIdCaseInsensitive;
    }

    @Override
    public List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        TableId tableId = getTableId(record);

        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        Map<String, String> meta = getMetadata(record);

        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            RecordData after = extractAfterDataRecord(tableId, value, valueSchema);
            return Collections.singletonList(DataChangeEvent.insertEvent(tableId, after, meta));
        } else if (op == Envelope.Operation.DELETE) {
            RecordData before = extractBeforeDataRecord(tableId, value, valueSchema);
            return Collections.singletonList(DataChangeEvent.deleteEvent(tableId, before, meta));
        } else if (op == Envelope.Operation.UPDATE) {
            RecordData after = extractAfterDataRecord(tableId, value, valueSchema);
            if (changelogMode == DebeziumChangelogMode.ALL) {
                RecordData before = extractBeforeDataRecord(tableId, value, valueSchema);
                return Collections.singletonList(
                        DataChangeEvent.updateEvent(tableId, before, after, meta));
            }
            return Collections.singletonList(
                    DataChangeEvent.updateEvent(tableId, null, after, meta));
        } else {
            LOG.trace("Received {} operation, skip", op);
            return Collections.emptyList();
        }
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        if (includeSchemaChanges) {
            if (customParser == null) {
                customParser =
                        new CustomMySqlAntlrDdlParser(
                                includeComments, tinyInt1isBit, isTableIdCaseInsensitive);
                tables = new Tables();
            }

            try {
                HistoryRecord historyRecord = getHistoryRecord(record);

                String databaseName =
                        historyRecord.document().getString(HistoryRecord.Fields.DATABASE_NAME);
                String ddl =
                        historyRecord.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
                customParser.setCurrentDatabase(databaseName);
                customParser.parse(ddl, tables);
                List<SchemaChangeEvent> schemaChangeEvents = customParser.getAndClearParsedEvents();
                clearDataTypeCache(schemaChangeEvents);
                return schemaChangeEvents;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse the schema change : " + record, e);
            }
        }
        getOrCreateDataTypeCache().clear();
        return Collections.emptyList();
    }

    private RecordData extractBeforeDataRecord(TableId tableId, Struct value, Schema valueSchema)
            throws Exception {
        Schema beforeSchema = fieldSchema(valueSchema, Envelope.FieldName.BEFORE);
        Struct beforeValue = fieldStruct(value, Envelope.FieldName.BEFORE);
        return extractDataRecord(tableId, beforeValue, beforeSchema);
    }

    private RecordData extractAfterDataRecord(TableId tableId, Struct value, Schema valueSchema)
            throws Exception {
        Schema afterSchema = fieldSchema(valueSchema, Envelope.FieldName.AFTER);
        Struct afterValue = fieldStruct(value, Envelope.FieldName.AFTER);
        return extractDataRecord(tableId, afterValue, afterSchema);
    }

    private RecordData extractDataRecord(TableId tableId, Struct value, Schema valueSchema)
            throws Exception {
        DataType dataType = getOrInferDataType(tableId, value, valueSchema);
        return convertDataRecord(value, valueSchema, dataType);
    }

    private DataType getOrInferDataType(TableId tableId, Struct value, Schema valueSchema) {
        ConcurrentMap<TableId, DataType> cache = getOrCreateDataTypeCache();
        DataType dataType = cache.get(tableId);
        if (dataType == null) {
            // Rows from the same table share the same schema until a schema change event arrives.
            dataType =
                    cache.computeIfAbsent(
                            tableId, key -> schemaDataTypeInference.infer(value, valueSchema));
        }
        return dataType;
    }

    private void clearDataTypeCache(List<SchemaChangeEvent> schemaChangeEvents) {
        ConcurrentMap<TableId, DataType> cache = getOrCreateDataTypeCache();
        for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
            // Keep cache invalidation aligned with the normalized cache key in case-insensitive
            // mode.
            cache.remove(normalizeTableId(schemaChangeEvent.tableId()));
        }
    }

    private ConcurrentMap<TableId, DataType> getOrCreateDataTypeCache() {
        if (dataTypeCache == null) {
            dataTypeCache = new ConcurrentHashMap<>();
        }
        return dataTypeCache;
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        Schema keySchema = record.keySchema();
        return keySchema != null && SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name());
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String dbName = source.getString(DATABASE_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return normalizeTableId(TableId.tableId(dbName, tableName));
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        readableMetadataList.forEach(
                (mySqlReadableMetadata -> {
                    Object metadata = mySqlReadableMetadata.getConverter().read(record);
                    if (mySqlReadableMetadata.equals(MySqlReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                mySqlReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(mySqlReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }

    @Override
    protected Object convertToString(Object dbzObj, Schema schema) {
        // the Geometry datatype in MySQL will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())) {
            try {
                Struct geometryStruct = (Struct) dbzObj;
                byte[] wkb = geometryStruct.getBytes("wkb");
                String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
                JsonNode originGeoNode = OBJECT_MAPPER.readTree(geoJson);
                Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
                Map<String, Object> geometryInfo = new HashMap<>();
                String geometryType = originGeoNode.get("type").asText();
                geometryInfo.put("type", geometryType);
                if (geometryType.equals("GeometryCollection")) {
                    geometryInfo.put("geometries", originGeoNode.get("geometries"));
                } else {
                    geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
                }
                geometryInfo.put("srid", srid.orElse(0));
                return BinaryStringData.fromString(
                        OBJECT_MAPPER.writer().writeValueAsString(geometryInfo));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", dbzObj), e);
            }
        } else {
            return BinaryStringData.fromString(dbzObj.toString());
        }
    }

    private TableId normalizeTableId(TableId tableId) {
        if (isTableIdCaseInsensitive) {
            return TableId.tableId(
                    tableId.getSchemaName().toLowerCase(Locale.ROOT),
                    tableId.getTableName().toLowerCase(Locale.ROOT));
        }
        return tableId;
    }
}
