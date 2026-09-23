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

package org.apache.flink.cdc.connectors.postgres.testutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresSourceRecordUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** Formatter that formats the {@link org.apache.kafka.connect.source.SourceRecord} to String. */
public class RecordsFormatter {

    private final DataType dataType;
    private final ZoneId zoneId;

    private TypeInformation<RowData> typeInfo;
    private DebeziumDeserializationSchema<RowData> deserializationSchema;
    private SimpleCollector collector;
    private RowRowConverter rowRowConverter;

    public RecordsFormatter(DataType dataType) {
        this(dataType, ZoneId.of("UTC"));
    }

    public RecordsFormatter(DataType dataType, ZoneId zoneId) {
        this.dataType = dataType;
        this.zoneId = zoneId;
        this.typeInfo =
                (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(dataType);
        this.deserializationSchema =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType((RowType) dataType.getLogicalType())
                        .setResultTypeInfo(typeInfo)
                        .build();
        this.collector = new SimpleCollector();
        this.rowRowConverter = RowRowConverter.create(dataType);
        rowRowConverter.open(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Formats records preserving the original order of both data change records and logical
     * messages.
     */
    public List<String> format(List<SourceRecord> records) {
        List<String> result = new ArrayList<>();
        for (SourceRecord r : records) {
            if (PostgresSourceRecordUtils.isLogicalMessage(r)) {
                result.add(formatLogicalMessage(r));
            } else if (SourceRecordUtils.isDataChangeRecord(r)) {
                try {
                    collector.list.clear();
                    deserializationSchema.deserialize(r, collector);
                    for (RowData rowData : collector.list) {
                        result.add(rowRowConverter.toExternal(rowData).toString());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return result;
    }

    private String formatLogicalMessage(SourceRecord record) {
        Struct message = ((Struct) record.value()).getStruct("message");
        String prefix = message.getString("prefix");
        Object contentObj = message.get("content");
        String content;
        if (contentObj instanceof ByteBuffer) {
            content = new String(((ByteBuffer) contentObj).array());
        } else {
            content = String.valueOf(contentObj);
        }

        return "M[" + prefix + ", " + content + "]";
    }

    private static class SimpleCollector implements Collector<RowData> {

        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
