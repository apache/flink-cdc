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
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getHistoryRecord;

/** Event deserializer for {@link MySqlDataSource}. */
@Internal
public class MySqlEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final boolean includeSchemaChanges;
    private final boolean tinyInt1isBit;
    private final boolean includeComments;

    private transient Tables tables;
    private transient CustomMySqlAntlrDdlParser customParser;

    private List<MySqlReadableMetadata> readableMetadataList;
    private boolean isTableIdCaseInsensitive;

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
        this.isTableIdCaseInsensitive = isTableIdCaseInsensitive;
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
                return customParser.getAndClearParsedEvents();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse the schema change : " + record, e);
            }
        }
        return Collections.emptyList();
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
        return TableId.tableId(dbName, tableName);
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
}
