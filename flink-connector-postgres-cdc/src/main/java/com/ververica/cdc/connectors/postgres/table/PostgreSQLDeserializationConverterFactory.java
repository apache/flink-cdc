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

package com.ververica.cdc.connectors.postgres.table;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Used to create {@link DeserializationRuntimeConverterFactory} specified to PostgreSQL. */
public class PostgreSQLDeserializationConverterFactory {

    public static final String SRID = "srid";
    public static final String HEXEWKB = "hexewkb";

    public static DeserializationRuntimeConverterFactory instance() {
        return new DeserializationRuntimeConverterFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public Optional<DeserializationRuntimeConverter> createUserDefinedConverter(
                    LogicalType logicalType, ZoneId serverTimeZone) {
                switch (logicalType.getTypeRoot()) {
                    case VARCHAR:
                        return createStringConverter();
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
                        // the Geometry datatype in PostgreSQL will be converted to
                        // a String with Json format
                        if (Geometry.LOGICAL_NAME.equals(schema.name())
                                || Geography.LOGICAL_NAME.equals(schema.name())) {
                            try {
                                Struct geometryStruct = (Struct) dbzObj;
                                byte[] wkb = geometryStruct.getBytes("wkb");
                                Optional<Integer> srid =
                                        Optional.ofNullable(geometryStruct.getInt32(SRID));
                                Map<String, Object> geometryInfo = new HashMap<>(2);
                                geometryInfo.put(HEXEWKB, HexConverter.convertToHexString(wkb));
                                geometryInfo.put(SRID, srid.orElse(0));
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
}
