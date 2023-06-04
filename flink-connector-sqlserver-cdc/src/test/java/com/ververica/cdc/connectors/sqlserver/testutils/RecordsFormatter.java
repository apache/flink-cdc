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

package com.ververica.cdc.connectors.sqlserver.testutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public List<String> format(List<SourceRecord> records) {
        records.stream()
                // Keep DataChangeEvent only
                .filter(SourceRecordUtils::isDataChangeRecord)
                .forEach(
                        r -> {
                            try {
                                deserializationSchema.deserialize(r, collector);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        return collector.list.stream()
                .map(rowRowConverter::toExternal)
                .map(Row::toString)
                .collect(Collectors.toList());
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
