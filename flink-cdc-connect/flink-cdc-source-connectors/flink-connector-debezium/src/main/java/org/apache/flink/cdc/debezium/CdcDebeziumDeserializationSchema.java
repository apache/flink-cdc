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

package org.apache.flink.cdc.debezium;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

/** CdcDebeziumDeserializationSchema. */
public class CdcDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = 2L;

    private final ObjectMapper objectMapper;

    public CdcDebeziumDeserializationSchema() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
            throws JsonProcessingException {
        Struct value = (Struct) sourceRecord.value();
        Struct source = value.getStruct("source");
        HashMap<String, String> sourceMap = new HashMap<>();
        HashMap<String, Object> afterMap = new HashMap<>();
        HashMap<String, Object> beforeMap = new HashMap<>();
        sourceMap.put("db", source.getString("db"));
        sourceMap.put("table", source.getString("table"));

        HashMap<String, Object> result = new HashMap<>();

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        Struct after = value.getStruct("after");
        Struct before = value.getStruct("before");
        switch (operation) {
            case READ:
            case CREATE:
                for (Field field : after.schema().fields()) {
                    afterMap.put(field.name(), after.getWithoutDefault(field.name()));
                }
                result.put("op", "c");
                break;
            case DELETE:
                for (Field field : before.schema().fields()) {
                    beforeMap.put(field.name(), before.getWithoutDefault(field.name()));
                }
                result.put("op", "d");
                break;
            case UPDATE:
                for (Field field : before.schema().fields()) {
                    beforeMap.put(field.name(), before.getWithoutDefault(field.name()));
                }
                for (Field field : after.schema().fields()) {
                    afterMap.put(field.name(), after.getWithoutDefault(field.name()));
                }
                result.put("op", "u");
                break;
            default:
                throw new RuntimeException("不存在改操作");
        }
        result.put("after", afterMap);
        result.put("before", beforeMap);
        result.put("source", sourceMap);

        String valueAsString = objectMapper.writeValueAsString(result);
        collector.collect(valueAsString);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
