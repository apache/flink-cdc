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

package com.ververica.cdc.connectors.table.deserializa;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;

/**
 * cdc json Deserialization.
 */
public class CdcJsonRowDataDebeziumDeserializationSchema implements DebeziumDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    private final Boolean includeSchema;

    /**
     * The custom configurations for {@link JsonConverter}.
     */
    private Map<String, Object> customConverterConfigs;

    public CdcJsonRowDataDebeziumDeserializationSchema() {
        this(false);
    }

    public CdcJsonRowDataDebeziumDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    public CdcJsonRowDataDebeziumDeserializationSchema(
            Boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs = customConverterConfigs;
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
    public TypeInformation<RowData> getProducedType() {
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("cdc_json", DataTypes.STRING())
                );
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);
        return resultTypeInfo;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<RowData> collector) throws Exception {
        if (jsonConverter == null) {
            initializeJsonConverter();
        }
        byte[] bytes =
                jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());
        String str = new String(bytes);
        collector.collect(GenericRowData.ofKind(RowKind.INSERT, StringData.fromString(str)));
    }
}
