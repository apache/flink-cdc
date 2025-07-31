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
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLReadableMetadata;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

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

    public static final String SRID = "srid";
    public static final String HEXEWKB = "hexewkb";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public PostgresEventDeserializer(DebeziumChangelogMode changelogMode) {
        super(new PostgresSchemaDataTypeInference(), changelogMode);
    }

    public PostgresEventDeserializer(
            DebeziumChangelogMode changelogMode,
            List<PostgreSQLReadableMetadata> readableMetadataList) {
        super(new PostgresSchemaDataTypeInference(), changelogMode);
        this.readableMetadataList = readableMetadataList;
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
        return TableId.tableId(parts[1], parts[2]);
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
        if (Geometry.LOGICAL_NAME.equals(schema.name())
                || Geography.LOGICAL_NAME.equals(schema.name())) {
            try {
                Struct geometryStruct = (Struct) dbzObj;
                byte[] wkb = geometryStruct.getBytes("wkb");
                Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32(SRID));
                Map<String, Object> geometryInfo = new HashMap<>(2);
                geometryInfo.put(HEXEWKB, HexConverter.convertToHexString(wkb));
                geometryInfo.put(SRID, srid.orElse(0));
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
