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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.oracle.source.parser.OracleAntlrDdlParser;
import org.apache.flink.cdc.connectors.oracle.table.OracleReadableMetaData;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geometry;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKBReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;

/** Event deserializer for {@link OracleDataSource}. */
@Internal
public class OracleEventDeserializer<Event> extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.oracle.SchemaChangeKey";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final boolean includeSchemaChanges;

    private transient Tables tables;

    private transient OracleAntlrDdlParser customParser;

    List<OracleReadableMetaData> readableMetadataList;

    public OracleEventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            List<OracleReadableMetaData> readableMetadataList) {
        super(new OracleSchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        if (includeSchemaChanges) {
            try {
                HistoryRecord historyRecord = getHistoryRecord(record);

                String databaseName =
                        historyRecord.document().getString(HistoryRecord.Fields.DATABASE_NAME);
                String schemaName =
                        historyRecord.document().getString(HistoryRecord.Fields.SCHEMA_NAME);
                if (customParser == null) {
                    customParser = new OracleAntlrDdlParser(databaseName, schemaName);
                    tables = new Tables();
                }
                String ddl =
                        historyRecord.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
                customParser.setCurrentDatabase(databaseName);
                customParser.parse(ddl, tables);
                return customParser.getAndClearParsedEvents();
            } catch (Exception e) {
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
        Map<String, String> map = new HashMap<>();
        readableMetadataList.forEach(
                (oracleReadableMetaData -> {
                    Object metadata = oracleReadableMetaData.getConverter().read(record);
                    if (oracleReadableMetaData.equals(OracleReadableMetaData.OP_TS)) {
                        map.put(
                                oracleReadableMetaData.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        map.put(oracleReadableMetaData.getKey(), String.valueOf(metadata));
                    }
                }));
        return map;
    }

    @Override
    protected Object convertToString(Object dbzObj, Schema schema) {
        // the Geometry datatype in oracle will be converted to
        // a String with Json format
        if (Geometry.LOGICAL_NAME.equals(schema.name())) {
            try {
                Struct geometryStruct = (Struct) dbzObj;
                byte[] wkb = geometryStruct.getBytes("wkb");
                WKBReader wkbReader = new WKBReader();
                org.locationtech.jts.geom.Geometry jtsGeom = wkbReader.read(wkb);

                Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
                Map<String, Object> geometryInfo = new HashMap<>();
                String geometryType = jtsGeom.getGeometryType();
                geometryInfo.put("type", geometryType);

                if (geometryType.equals("GeometryCollection")) {
                    geometryInfo.put("geometries", jtsGeom.toText());
                } else {
                    Coordinate[] coordinates = jtsGeom.getCoordinates();
                    List<double[]> coordinateList = new ArrayList<>();
                    if (coordinates != null) {
                        for (Coordinate coordinate : coordinates) {
                            coordinateList.add(new double[] {coordinate.x, coordinate.y});
                            geometryInfo.put(
                                    "coordinates", new double[] {coordinate.x, coordinate.y});
                        }
                    }
                    geometryInfo.put(
                            "coordinates",
                            OBJECT_MAPPER.writer().writeValueAsString(coordinateList));
                }
                geometryInfo.put("srid", srid.orElse(0));
                return BinaryStringData.fromString(OBJECT_MAPPER.writeValueAsString(geometryInfo));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", dbzObj), e);
            }
        } else {
            return BinaryStringData.fromString(dbzObj.toString());
        }
    }
}
