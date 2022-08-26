/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import io.debezium.data.EnumSet;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Used to create {@link DeserializationRuntimeConverterFactory} specified to MySQL. */
public class MySqlDeserializationConverterFactory {

    public static DeserializationRuntimeConverterFactory instance() {
        return new DeserializationRuntimeConverterFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public Optional<DeserializationRuntimeConverter> createUserDefinedConverter(
                    LogicalType logicalType, ZoneId serverTimeZone) {
                switch (logicalType.getTypeRoot()) {
                    case CHAR:
                    case VARCHAR:
                        return createStringConverter();
                    case ARRAY:
                        return createArrayConverter((ArrayType) logicalType);
                    default:
                        // fallback to default converter
                        return Optional.empty();
                }
            }
        };
    }

    private static Optional<DeserializationRuntimeConverter> createStringConverter() {
        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectWriter objectWriter = objectMapper.writer();
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        // the Geometry datatype in MySQL will be converted to
                        // a String with Json format
                        if (Point.LOGICAL_NAME.equals(schema.name())
                                || Geometry.LOGICAL_NAME.equals(schema.name())) {
                            try {
                                Struct geometryStruct = (Struct) dbzObj;
                                byte[] wkb = geometryStruct.getBytes("wkb");
                                String geoJson =
                                        OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
                                JsonNode originGeoNode = objectMapper.readTree(geoJson);
                                Optional<Integer> srid =
                                        Optional.ofNullable(geometryStruct.getInt32("srid"));
                                Map<String, Object> geometryInfo = new HashMap<>();
                                String geometryType = originGeoNode.get("type").asText();
                                geometryInfo.put("type", geometryType);
                                if (geometryType.equals("GeometryCollection")) {
                                    geometryInfo.put("geometries", originGeoNode.get("geometries"));
                                } else {
                                    geometryInfo.put(
                                            "coordinates", originGeoNode.get("coordinates"));
                                }
                                geometryInfo.put("srid", srid.orElse(0));
                                return StringData.fromString(
                                        objectWriter.writeValueAsString(geometryInfo));
                            } catch (Exception e) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Failed to convert %s to geometry JSON.", dbzObj),
                                        e);
                            }
                        } else {
                            return StringData.fromString(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createArrayConverter(
            ArrayType arrayType) {
        if (hasFamily(arrayType.getElementType(), LogicalTypeFamily.CHARACTER_STRING)) {
            // only map MySQL SET type to Flink ARRAY<STRING> type
            return Optional.of(
                    new DeserializationRuntimeConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(Object dbzObj, Schema schema) throws Exception {
                            if (EnumSet.LOGICAL_NAME.equals(schema.name())
                                    && dbzObj instanceof String) {
                                // for SET datatype in mysql, debezium will always
                                // return a string split by comma like "a,b,c"
                                String[] enums = ((String) dbzObj).split(",");
                                StringData[] elements = new StringData[enums.length];
                                for (int i = 0; i < enums.length; i++) {
                                    elements[i] = StringData.fromString(enums[i]);
                                }
                                return new GenericArrayData(elements);
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Unable convert to Flink ARRAY type from unexpected value '%s', "
                                                        + "only SET type could be converted to ARRAY type for MySQL",
                                                dbzObj));
                            }
                        }
                    });
        } else {
            // otherwise, fallback to default converter
            return Optional.empty();
        }
    }

    private static boolean hasFamily(LogicalType logicalType, LogicalTypeFamily family) {
        return logicalType.getTypeRoot().getFamilies().contains(family);
    }
}
