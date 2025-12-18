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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import io.debezium.data.Json;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GaussDBDeserializationConverterFactoryTest {

    @Test
    void convertsJsonToStringWithEscaping() throws Exception {
        DeserializationRuntimeConverter converter =
                GaussDBDeserializationConverterFactory.instance()
                        .createUserDefinedConverter(
                                new VarCharType(VarCharType.MAX_LENGTH), ZoneId.of("UTC"))
                        .orElseThrow(IllegalStateException::new);

        Schema jsonSchema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", "b\nc");

        Object converted = converter.convert(map, jsonSchema);
        assertThat(converted).isEqualTo(StringData.fromString("{\"a\":\"b\\nc\"}"));
    }

    @Test
    void convertsByteaHexStringToBytes() throws Exception {
        DeserializationRuntimeConverter converter =
                GaussDBDeserializationConverterFactory.instance()
                        .createUserDefinedConverter(
                                new VarBinaryType(VarBinaryType.MAX_LENGTH), ZoneId.of("UTC"))
                        .orElseThrow(IllegalStateException::new);

        byte[] bytes = (byte[]) converter.convert("\\x0A0B", null);
        assertThat(bytes).containsExactly((byte) 0x0A, (byte) 0x0B);
    }

    @Test
    void keepsNonHexBinaryStringBehavior() throws Exception {
        DeserializationRuntimeConverter converter =
                GaussDBDeserializationConverterFactory.instance()
                        .createUserDefinedConverter(
                                new VarBinaryType(VarBinaryType.MAX_LENGTH), ZoneId.of("UTC"))
                        .orElseThrow(IllegalStateException::new);

        byte[] bytes = (byte[]) converter.convert("YWJj", null);
        assertThat(bytes).isEqualTo("YWJj".getBytes(StandardCharsets.UTF_8));
    }
}
