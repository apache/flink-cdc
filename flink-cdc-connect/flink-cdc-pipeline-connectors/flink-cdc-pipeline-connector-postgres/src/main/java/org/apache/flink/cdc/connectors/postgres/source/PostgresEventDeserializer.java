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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKBReader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Event deserializer for {@link PostgresDataSource}. */
@Internal
public class PostgresEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;
    private List<PostgreSQLReadableMetadata> readableMetadataList;
    private final boolean includeDatabaseInTableId;
    private final String databaseName;
    private final String unavailableValuePlaceholder;
    private final byte[] unavailableValuePlaceholderBytes;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public PostgresEventDeserializer(DebeziumChangelogMode changelogMode) {
        this(changelogMode, new ArrayList<>(), false, null);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList) {
        this(changelogMode, readableMetadataList, false, null);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList,
            boolean includeDatabaseInTableId) {
        this(changelogMode, readableMetadataList, includeDatabaseInTableId, null);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList,
            boolean includeDatabaseInTableId,
            String databaseName) {
        this(
                changelogMode,
                readableMetadataList,
                includeDatabaseInTableId,
                databaseName,
                RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList,
            boolean includeDatabaseInTableId,
            String databaseName,
            String unavailableValuePlaceholder) {
        super(new PostgresSchemaDataTypeInference(), changelogMode);
        this.readableMetadataList = readableMetadataList;
        this.includeDatabaseInTableId = includeDatabaseInTableId;
        this.databaseName = databaseName;
        this.unavailableValuePlaceholder = unavailableValuePlaceholder;
        // Debezium encodes the placeholder of binary columns with the platform charset
        this.unavailableValuePlaceholderBytes = unavailableValuePlaceholder.getBytes();
    }

    @Override
    public List<DataChangeEvent> deserializeDataChangeRecord(SourceRecord record) throws Exception {
        if (changelogMode == DebeziumChangelogMode.UPSERT) {
            Envelope.Operation op = Envelope.operationFor(record);
            // A primary key change is emitted as a DELETE and a CREATE carrying the new row.
            if (op == Envelope.Operation.UPDATE || op == Envelope.Operation.CREATE) {
                checkAfterValuesAvailable(record);
            }
        }
        return super.deserializeDataChangeRecord(record);
    }

    /**
     * A REPLACE or INSERT event overwrites the whole row, so an unchanged TOASTed column that
     * PostgreSQL does not send under REPLICA IDENTITY DEFAULT must not be emitted as its
     * placeholder.
     */
    private void checkAfterValuesAvailable(SourceRecord record) {
        Struct after = fieldStruct((Struct) record.value(), Envelope.FieldName.AFTER);
        if (after == null) {
            return;
        }
        for (Field field : after.schema().fields()) {
            Preconditions.checkState(
                    !isUnavailableValue(after.get(field)),
                    "The value of column '%s' in table %s is unavailable in upsert changelog "
                            + "mode. This happens when a TOASTed column is unchanged by an UPDATE "
                            + "while the table is not configured with REPLICA IDENTITY FULL. "
                            + "Configure REPLICA IDENTITY FULL for the table.",
                    field.name(),
                    getTableId(record));
        }
    }

    private boolean isUnavailableValue(Object value) {
        if (value instanceof String) {
            return unavailableValuePlaceholder.equals(value);
        }
        if (value instanceof byte[]) {
            return Arrays.equals(unavailableValuePlaceholderBytes, (byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            return ByteBuffer.wrap(unavailableValuePlaceholderBytes).equals(value);
        }
        return false;
    }

    @Override
    protected RecordData extractBeforeDataRecord(Struct value, Schema valueSchema)
            throws Exception {
        Struct beforeValue = fieldStruct(value, Envelope.FieldName.BEFORE);
        Preconditions.checkNotNull(
                beforeValue,
                "Before data is null for UPDATE/DELETE event. "
                        + "This usually happens when the source table is not configured with "
                        + "REPLICA IDENTITY FULL. Schema name: "
                        + valueSchema.name());
        return super.extractBeforeDataRecord(value, valueSchema);
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
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
        return false;
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        String[] parts = record.topic().split("\\.");
        if (includeDatabaseInTableId && databaseName != null) {
            return TableId.tableId(databaseName, parts[1], parts[2]);
        } else {
            return TableId.tableId(parts[1], parts[2]);
        }
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        readableMetadataList.forEach(
                (postgresReadableMetadata -> {
                    Object metadata = postgresReadableMetadata.getConverter().read(record);
                    if (postgresReadableMetadata.equals(PostgreSQLReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                postgresReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(
                                postgresReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                }));
        return metadataMap;
    }

    @Override
    protected Object convertToString(Object dbzObj, Schema schema) {
        // the Geometry datatype in PostgreSQL will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())
                || Geography.LOGICAL_NAME.equals(schema.name())) {
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
                            "coordinates", OBJECT_MAPPER.writeValueAsString(coordinateList));
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
