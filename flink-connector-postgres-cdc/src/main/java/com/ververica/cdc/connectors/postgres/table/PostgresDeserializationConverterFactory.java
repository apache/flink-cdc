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

package com.ververica.cdc.connectors.postgres.table;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.ZoneId;
import java.util.Optional;

import static org.apache.flink.util.StringUtils.byteToHexString;

/** Used to create {@link DeserializationRuntimeConverterFactory} specified to Postgres. */
public class PostgresDeserializationConverterFactory {

    public static boolean isGeometrySchema(String schema) {
        switch (schema) {
            case Point.LOGICAL_NAME:
            case Geometry.LOGICAL_NAME:
            case Geography.LOGICAL_NAME:
                return true;
        }

        return false;
    }

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
                    case BINARY:
                    case VARBINARY:
                        return createBinaryConverter();
                    default:
                        // fallback to default converter
                        return Optional.empty();
                }
            }
        };
    }

    private static Optional<DeserializationRuntimeConverter> createBinaryConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        if (isGeometrySchema(schema.name())) {
                            // The stuct consists of 2 fields the wkb byte array and the srid
                            // specifier
                            Struct geometryStruct = (Struct) dbzObj;
                            // The byte array is actually ewkb thus srid should be encoded in
                            // it in case of a geography object. See:
                            // https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgisGeometry.java
                            return geometryStruct.getBytes("wkb");
                        } else {
                            return StringData.fromString(dbzObj.toString());
                        }
                    }
                });
    }

    private static Optional<DeserializationRuntimeConverter> createStringConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {

                        if (isGeometrySchema(schema.name())) {
                            // The stuct consists of 2 fields the wkb byte array and the srid
                            // specifier
                            Struct geometryStruct = (Struct) dbzObj;
                            // The byte array is actually ewkb thus srid should be encoded in
                            // it in case of a geography object. See
                            // https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgisGeometry.java
                            byte[] wkb = geometryStruct.getBytes("wkb");
                            String s = byteToHexString(wkb);
                            return StringData.fromString(s);
                        } else {
                            return StringData.fromString(dbzObj.toString());
                        }
                    }
                });
    }
}
