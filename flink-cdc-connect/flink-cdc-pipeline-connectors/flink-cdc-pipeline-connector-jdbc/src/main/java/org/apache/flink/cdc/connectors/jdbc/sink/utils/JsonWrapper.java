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

package org.apache.flink.cdc.connectors.jdbc.sink.utils;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;

/** JsonWrapper utils. */
public class JsonWrapper implements Serializable {
    private transient ObjectMapper objectMapper;

    public JsonWrapper() {
        objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(BinaryStringData.class, new BinaryStringDataSerializer());
        module.addSerializer(DecimalData.class, new DecimalDataSerializer());
        module.addSerializer(TimestampData.class, new TimestampDataSerializer());
        objectMapper.registerModule(module);
    }

    public byte[] toJSONBytes(Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(object);
    }

    public String toJSONString(Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    public <T> T parse(String text, Class<T> clazz) throws JsonProcessingException {
        return objectMapper.readValue(text, clazz);
    }

    public <T> T parseObject(byte[] bytes, Class<T> clazz) throws IOException {
        return objectMapper.readValue(bytes, clazz);
    }

    // Add method to parse using TypeReference
    public <T> T parseObject(byte[] bytes, TypeReference<T> typeReference) throws IOException {
        return objectMapper.readValue(bytes, typeReference);
    }

    // Method to parse String using TypeReference
    public <T> T parseObject(String text, TypeReference<T> typeReference) throws IOException {
        return objectMapper.readValue(text, typeReference);
    }

    // BinaryStringData Serializer
    static final class BinaryStringDataSerializer extends JsonSerializer<BinaryStringData> {
        @Override
        public void serialize(
                BinaryStringData value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(value.toString());
        }
    }

    // DecimalData Serializer
    static final class DecimalDataSerializer extends JsonSerializer<DecimalData> {
        @Override
        public void serialize(DecimalData value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(value.toBigDecimal().toPlainString());
        }
    }

    // TimestampData Serializer
    static final class TimestampDataSerializer extends JsonSerializer<TimestampData> {
        private final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

        @Override
        public void serialize(
                TimestampData value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(value.toLocalDateTime().format(formatter));
        }
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            SimpleModule module = new SimpleModule();
            module.addSerializer(BinaryStringData.class, new BinaryStringDataSerializer());
            module.addSerializer(DecimalData.class, new DecimalDataSerializer());
            module.addSerializer(TimestampData.class, new TimestampDataSerializer());
            objectMapper.registerModule(module);
        }
    }
}
