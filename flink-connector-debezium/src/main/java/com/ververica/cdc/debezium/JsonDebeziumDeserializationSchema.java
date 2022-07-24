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

package com.ververica.cdc.debezium;

import com.ververica.cdc.debezium.utils.SourceRecordUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;

/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which deserializes the
 * received {@link SourceRecord} to JSON String.
 */
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    /**
     * Configuration whether to enable {@link JsonConverterConfig#SCHEMAS_ENABLE_CONFIG} to include
     * schema in messages.
     */
    private final Boolean includeSchema;
    private final int timeZoneOffset;

    /**
     * The custom configurations for {@link JsonConverter}.
     */
    private Map<String, Object> customConverterConfigs;

    public JsonDebeziumDeserializationSchema() {
        this(false);
    }

    public JsonDebeziumDeserializationSchema(Boolean includeSchema) {
        this(includeSchema, 8);
    }

    public JsonDebeziumDeserializationSchema(Boolean includeSchema, int timeZoneOffset) {
        this.includeSchema = includeSchema;
        this.timeZoneOffset = timeZoneOffset;
    }

    public JsonDebeziumDeserializationSchema(
            Boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this(includeSchema, customConverterConfigs, 8);
    }

    public JsonDebeziumDeserializationSchema(
            Boolean includeSchema, Map<String, Object> customConverterConfigs, int timeZoneOffset) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs = customConverterConfigs;
        this.timeZoneOffset = timeZoneOffset;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) {
        if (jsonConverter == null) {
            initializeJsonConverter();
        }
        // correct the record time zone
        SourceRecord sourceRecord = SourceRecordUtil.correctTimeZoneSourceRecord(record, timeZoneOffset);
        byte[] bytes =
                jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());
        out.collect(new String(bytes));
    }

    /**
     * Initialize {@link JsonConverter} with given configs.
     */
    private void initializeJsonConverter() {
        jsonConverter = new JsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        if (customConverterConfigs != null) {
            configs.putAll(customConverterConfigs);
        }
        jsonConverter.configure(configs);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
