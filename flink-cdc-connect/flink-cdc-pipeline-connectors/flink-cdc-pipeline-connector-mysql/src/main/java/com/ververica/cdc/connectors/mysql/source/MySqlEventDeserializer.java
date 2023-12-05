/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.connectors.mysql.source.parser.CustomMySqlAntlrDdlParser;
import com.ververica.cdc.debezium.event.DebeziumEventDeserializationSchema;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getHistoryRecord;

/** Event deserializer for {@link MySqlDataSource}. */
@Internal
public class MySqlEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final boolean includeSchemaChanges;

    private transient Tables tables;
    private transient CustomMySqlAntlrDdlParser customParser;

    public MySqlEventDeserializer(
            DebeziumChangelogMode changelogMode, boolean includeSchemaChanges) {
        super(new MySqlSchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        if (includeSchemaChanges) {
            if (customParser == null) {
                customParser = new CustomMySqlAntlrDdlParser();
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
        String[] parts = record.topic().split("\\.");
        return TableId.tableId(parts[1], parts[2]);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        return Collections.emptyMap();
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
