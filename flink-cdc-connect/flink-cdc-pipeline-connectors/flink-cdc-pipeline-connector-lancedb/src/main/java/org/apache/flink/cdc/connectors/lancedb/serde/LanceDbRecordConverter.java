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

package org.apache.flink.cdc.connectors.lancedb.serde;

import org.apache.flink.cdc.common.converter.JavaObjectConverter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkConfig;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbPathUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/** Converts Flink CDC records to Lance row values. */
public class LanceDbRecordConverter {

    private final Schema schema;
    private final LanceDbDataSinkConfig config;
    private final List<RecordData.FieldGetter> fieldGetters;
    private final boolean includeCdcMetadata;

    public LanceDbRecordConverter(
            Schema schema, LanceDbDataSinkConfig config, boolean includeCdcMetadata) {
        this.schema = schema;
        this.config = config;
        this.fieldGetters = SchemaUtils.createFieldGetters(schema);
        this.includeCdcMetadata = includeCdcMetadata;
    }

    public LanceDbOperation convert(DataChangeEvent event) {
        RecordData record = selectRecord(event);
        if (record == null) {
            throw new IllegalStateException(
                    event.op() + " event does not contain the required record.");
        }
        List<Object> values = convertRecord(record);
        if (includeCdcMetadata) {
            values.add(event.op().name());
            values.add(event.op() == OperationType.DELETE);
            values.add(System.currentTimeMillis());
        }
        return new LanceDbOperation(
                LanceDbPathUtils.resolveDatasetPath(event.tableId(), config), event.op(), values);
    }

    public List<Object> convertRecord(RecordData record) {
        List<Object> values = new ArrayList<>();
        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (!column.isPhysical()) {
                continue;
            }
            Object value = fieldGetters.get(i).getFieldOrNull(record);
            values.add(convertValue(value, column.getType()));
        }
        return values;
    }

    private RecordData selectRecord(DataChangeEvent event) {
        if (event.op() == OperationType.DELETE) {
            return event.before();
        }
        return event.after();
    }

    private Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        Object javaValue = JavaObjectConverter.convertToJava(value, dataType);
        switch (dataType.getTypeRoot()) {
            case DATE:
                return (int) ((LocalDate) javaValue).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (((LocalTime) javaValue).toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return toMicros(((LocalDateTime) javaValue).atZone(config.getZoneId()).toInstant());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return toMicros((Instant) javaValue);
            case TIMESTAMP_WITH_TIME_ZONE:
                return toMicros(
                        ((ZonedDateTime) javaValue)
                                .withZoneSameInstant(config.getZoneId())
                                .toInstant());
            case DECIMAL:
                return (BigDecimal) javaValue;
            case ARRAY:
                return javaValue;
            case MAP:
            case ROW:
            case VARIANT:
                throw new UnsupportedOperationException(
                        "LanceDB connector does not support "
                                + dataType.asSummaryString()
                                + " values yet.");
            default:
                return javaValue;
        }
    }

    private long toMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }
}
