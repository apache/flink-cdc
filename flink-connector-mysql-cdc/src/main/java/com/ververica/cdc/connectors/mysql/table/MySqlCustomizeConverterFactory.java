/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Used to create factories of {@link DeserializationRuntimeConverterFactory} specified to MySQL.
 */
public class MySqlCustomizeConverterFactory {

    public static Map<LogicalTypeRoot, DeserializationRuntimeConverterFactory>
            createCustomizeConverterFactories(LogicalType logicalType) {
        Map<LogicalTypeRoot, DeserializationRuntimeConverterFactory> converterFactories =
                new HashMap<>();
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                converterFactories.put(logicalType.getTypeRoot(), createStringConverterFactory());
                break;
            case ARRAY:
                converterFactories.put(logicalType.getTypeRoot(), createArrayConverterFactory());
                break;
            case ROW:
                ((RowType) logicalType)
                        .getFields().stream()
                                .map(RowType.RowField::getType)
                                .forEach(
                                        (logicType) ->
                                                converterFactories.putAll(
                                                        createCustomizeConverterFactories(
                                                                logicType)));
        }
        return converterFactories;
    }

    private static DeserializationRuntimeConverterFactory createStringConverterFactory() {
        return new DeserializationRuntimeConverterFactory() {
            private static final long serialVersionUID = 1L;
            final ObjectMapper objectMapper = new ObjectMapper();
            final ObjectWriter objectWriter = objectMapper.writer();

            @Override
            public DeserializationRuntimeConverter create(
                    LogicalType logicalType,
                    Function<LogicalType, DeserializationRuntimeConverter> converterFunc) {
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
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
                                Map<String, Object> geometryInfo =
                                        new HashMap<String, Object>() {
                                            {
                                                put("type", originGeoNode.get("type"));
                                                put(
                                                        "coordinates",
                                                        originGeoNode.get("coordinates"));
                                                put("srid", srid.orElse(0));
                                            }
                                        };
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
                };
            }
        };
    }

    private static DeserializationRuntimeConverterFactory createArrayConverterFactory() {
        return new DeserializationRuntimeConverterFactory() {
            @Override
            public DeserializationRuntimeConverter create(
                    LogicalType logicalType,
                    Function<LogicalType, DeserializationRuntimeConverter> converterFunc) {
                ArrayType arrayType = (ArrayType) logicalType;
                final Class<?> elementClass =
                        LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
                final DeserializationRuntimeConverter elementConverter =
                        converterFunc.apply(arrayType.getElementType());
                return new DeserializationRuntimeConverter() {
                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        if (EnumSet.LOGICAL_NAME.equals(schema.name())
                                && dbzObj instanceof String) {
                            // for SET datatype in mysql, debezium will always
                            // return a string split by comma
                            // like "a,b,c"
                            // like "a,b,c"
                            String[] elements = ((String) dbzObj).split(",");
                            final Object[] elementArray =
                                    (Object[]) Array.newInstance(elementClass, elements.length);
                            for (int i = 0; i < elements.length; i++) {
                                elementArray[i] = elementConverter.convert(elements[i], schema);
                            }
                            return new GenericArrayData(elementArray);
                        } else {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Unable convert to Flink ARRAY type from unexpected value '%s', only SET type could convert to ARRAY type for MySQL",
                                            dbzObj));
                        }
                    }
                };
            }
        };
    }
}
