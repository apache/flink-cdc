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

package com.ververica.cdc.debezium;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which deserializes the
 * received {@link SourceRecord} to JSON String.
 */
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    private static final JsonConverter CONVERTER =  new JsonConverter();

    private static final HashMap<String, Object> CONFIGS = new HashMap<>();

    /** Configuration {@link JsonConverterConfig.SCHEMAS_ENABLE_CONFIG} enabled to include schema in the message. */
    private final Boolean includeSchema;

    /** When the deserialize method is first called, Configure CONVERTER. */
    private static Boolean isFirst = true;

    public JsonDebeziumDeserializationSchema() {
        this(false);
    }

    public JsonDebeziumDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        // Avoid occurred NullPointException in CONVERTER.FromConnectData() was performed when deploy on the cluster
        // and can be instantiated by {@ link JsonDebeziumDeserializationSchema} constructor
        // to control whether to enable or disable inclusion patterns in messages.
        if (isFirst) {
            CONFIGS.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
            CONFIGS.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
            CONVERTER.configure(CONFIGS);
            isFirst = false;
        }
        byte[] bytes =
            CONVERTER.fromConnectData(record.topic(), record.valueSchema(), record.value());
        out.collect(new String(bytes));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
