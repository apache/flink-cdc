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

package org.apache.flink.cdc.connectors.gaussdb.table;

import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverterFactory;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.debezium.data.Json;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Used to create {@link DeserializationRuntimeConverterFactory} specified to GaussDB. */
public class GaussDBDeserializationConverterFactory {

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

    private static Optional<DeserializationRuntimeConverter> createStringConverter() {
        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectWriter objectWriter = objectMapper.writer();
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        // JSON/JSONB should always be converted to STRING.
                        if (schema != null && Json.LOGICAL_NAME.equals(schema.name())) {
                            return StringData.fromString(
                                    convertJsonLikeToString(dbzObj, schema, objectWriter));
                        }
                        return StringData.fromString(convertAnyToString(dbzObj));
                    }
                });
    }

    private static String convertAnyToString(Object value) throws Exception {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof CharSequence) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) value;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    private static String convertJsonLikeToString(Object value, Schema schema, ObjectWriter writer)
            throws Exception {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            // Debezium (and many JDBC drivers) represent JSON/JSONB as a string already.
            return (String) value;
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return convertAnyToString(value);
        }
        if (value instanceof Struct) {
            // Convert Struct to JSON to guarantee correct escaping.
            Struct struct = (Struct) value;
            Map<String, Object> map = new LinkedHashMap<>();
            for (Field field : schema.fields()) {
                map.put(field.name(), struct.get(field));
            }
            return writer.writeValueAsString(map);
        }
        if (value instanceof Map) {
            return writer.writeValueAsString(value);
        }
        // Fallback: serialize as JSON to guarantee correct escaping of special characters.
        return writer.writeValueAsString(value);
    }

    private static Optional<DeserializationRuntimeConverter> createBinaryConverter() {
        return Optional.of(
                new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        if (dbzObj instanceof byte[]) {
                            return dbzObj;
                        }
                        if (dbzObj instanceof ByteBuffer) {
                            ByteBuffer buffer = (ByteBuffer) dbzObj;
                            byte[] bytes = new byte[buffer.remaining()];
                            buffer.get(bytes);
                            return bytes;
                        }
                        if (dbzObj instanceof String) {
                            // GaussDB/PostgreSQL BYTEA may be rendered as hex string like
                            // "\\xDEADBEEF".
                            String s = ((String) dbzObj).trim();
                            if (s.startsWith("\\\\x")
                                    || s.startsWith("\\x")
                                    || s.startsWith("0x")
                                    || s.startsWith("0X")) {
                                return parseHexByteString(s);
                            }
                            // Keep the same behavior as the default converter for other string
                            // encodings.
                            return s.getBytes(StandardCharsets.UTF_8);
                        }
                        // Fallback to default binary representation.
                        return dbzObj.toString().getBytes(StandardCharsets.UTF_8);
                    }
                });
    }

    private static byte[] parseHexByteString(String value) {
        String s = value.trim();
        if (s.startsWith("\\\\x") || s.startsWith("\\x")) {
            s = s.substring(s.startsWith("\\\\x") ? 3 : 2);
        } else if (s.startsWith("0x") || s.startsWith("0X")) {
            s = s.substring(2);
        }
        if ((s.length() & 1) != 0) {
            throw new IllegalArgumentException("Invalid hex string length for BYTEA: " + value);
        }
        int len = s.length() / 2;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            int hi = Character.digit(s.charAt(i * 2), 16);
            int lo = Character.digit(s.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("Invalid hex character in BYTEA: " + value);
            }
            bytes[i] = (byte) ((hi << 4) + lo);
        }
        return bytes;
    }
}
