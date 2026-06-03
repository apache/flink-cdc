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

package org.apache.flink.cdc.connectors.tdengine.serde;

import org.apache.flink.cdc.common.converter.JavaObjectConverter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkConfig;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineNameUtils;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineSqlUtils;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineTableInfo;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Converts Flink CDC records to TDengine row data. */
public class TDengineRowConverter {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final TableId tableId;
    private final Schema schema;
    private final TDengineDataSinkConfig config;
    private final TDengineTableInfo tableInfo;
    private final List<RecordData.FieldGetter> fieldGetters;
    private final int timestampIndex;
    private final int subtableIndex;
    private final List<Integer> tagIndexes;

    public TDengineRowConverter(TableId tableId, Schema schema, TDengineDataSinkConfig config) {
        this.tableId = tableId;
        this.schema = schema;
        this.config = config;
        this.tableInfo = TDengineSqlUtils.resolveTableInfo(tableId, config);
        this.fieldGetters = SchemaUtils.createFieldGetters(schema);
        this.timestampIndex = requireColumnIndex(config.getTimestampField());
        this.subtableIndex = requireColumnIndex(config.getSubtableField());
        this.tagIndexes = new ArrayList<>();
        for (String tagField : config.getTagFields()) {
            tagIndexes.add(requireColumnIndex(tagField));
        }
    }

    public TDengineRowData convert(RecordData record) {
        Object timestampValue =
                normalizeTimestamp(
                        extractField(record, timestampIndex),
                        schema.getColumns().get(timestampIndex).getType(),
                        config.getZoneId());
        if (timestampValue == null) {
            throw new IllegalArgumentException(
                    "TDengine timestamp.field "
                            + config.getTimestampField()
                            + " must not be null.");
        }

        Object subtableValue =
                convertValue(
                        extractField(record, subtableIndex),
                        schema.getColumns().get(subtableIndex).getType());
        String subtableName = normalizeSubtableName(subtableValue);

        List<String> tagColumnNames = new ArrayList<>();
        List<Object> tagValues = new ArrayList<>();
        for (int tagIndex : tagIndexes) {
            Column column = schema.getColumns().get(tagIndex);
            tagColumnNames.add(column.getName());
            tagValues.add(convertValue(extractField(record, tagIndex), column.getType()));
        }

        List<String> metricColumnNames = new ArrayList<>();
        List<Object> metricValues = new ArrayList<>();
        metricColumnNames.add(config.getTimestampField());
        metricValues.add(timestampValue);
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column column = schema.getColumns().get(i);
            if (!column.isPhysical()
                    || i == timestampIndex
                    || i == subtableIndex
                    || config.getTagFields().contains(column.getName())) {
                continue;
            }
            metricColumnNames.add(column.getName());
            metricValues.add(convertValue(extractField(record, i), column.getType()));
        }

        return new TDengineRowData(
                tableInfo,
                subtableName,
                timestampValue,
                tagColumnNames,
                tagValues,
                metricColumnNames,
                metricValues);
    }

    public boolean timestampEquals(RecordData left, RecordData right) {
        Object leftValue =
                normalizeTimestamp(
                        extractField(left, timestampIndex),
                        schema.getColumns().get(timestampIndex).getType(),
                        config.getZoneId());
        Object rightValue =
                normalizeTimestamp(
                        extractField(right, timestampIndex),
                        schema.getColumns().get(timestampIndex).getType(),
                        config.getZoneId());
        return Objects.equals(leftValue, rightValue);
    }

    public boolean subtableEquals(RecordData left, RecordData right) {
        Object leftValue =
                convertValue(
                        extractField(left, subtableIndex),
                        schema.getColumns().get(subtableIndex).getType());
        Object rightValue =
                convertValue(
                        extractField(right, subtableIndex),
                        schema.getColumns().get(subtableIndex).getType());
        return Objects.equals(normalizeSubtableName(leftValue), normalizeSubtableName(rightValue));
    }

    public Object extractSubtableValue(RecordData record) {
        return convertValue(
                extractField(record, subtableIndex),
                schema.getColumns().get(subtableIndex).getType());
    }

    public String extractNormalizedSubtableName(RecordData record) {
        return normalizeSubtableName(extractSubtableValue(record));
    }

    private int requireColumnIndex(String columnName) {
        int index = schema.getColumnNames().indexOf(columnName);
        if (index < 0) {
            throw new IllegalArgumentException(
                    "Column " + columnName + " is absent from table " + tableId + " schema.");
        }
        return index;
    }

    private Object extractField(RecordData record, int index) {
        return fieldGetters.get(index).getFieldOrNull(record);
    }

    private Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        Object javaValue = JavaObjectConverter.convertToJava(value, dataType);
        if (javaValue instanceof LocalDate
                || javaValue instanceof LocalTime
                || javaValue instanceof LocalDateTime
                || javaValue instanceof Instant
                || javaValue instanceof ZonedDateTime) {
            return normalizeTimestamp(javaValue, dataType, config.getZoneId());
        }
        return javaValue;
    }

    private String normalizeSubtableName(Object value) {
        if (value == null || value.toString().trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "TDengine subtable.field "
                            + config.getSubtableField()
                            + " must not be null or blank.");
        }
        return config.isNameNormalizeEnabled()
                ? TDengineNameUtils.normalizeIdentifier(value.toString(), "subtable")
                : TDengineNameUtils.validateIdentifier(value.toString(), "subtable");
    }

    private static Object normalizeTimestamp(Object value, DataType dataType, ZoneId zoneId) {
        if (value == null) {
            return null;
        }
        Object javaValue = JavaObjectConverter.convertToJava(value, dataType);
        if (javaValue instanceof Number) {
            return javaValue;
        }
        if (javaValue instanceof LocalDate) {
            return ((LocalDate) javaValue).atStartOfDay(zoneId).format(TIMESTAMP_FORMATTER);
        }
        if (javaValue instanceof LocalTime) {
            return LocalDate.ofEpochDay(0)
                    .atTime((LocalTime) javaValue)
                    .atZone(zoneId)
                    .format(TIMESTAMP_FORMATTER);
        }
        if (javaValue instanceof LocalDateTime) {
            return ((LocalDateTime) javaValue).format(TIMESTAMP_FORMATTER);
        }
        if (javaValue instanceof Instant) {
            return ((Instant) javaValue).atZone(zoneId).format(TIMESTAMP_FORMATTER);
        }
        if (javaValue instanceof ZonedDateTime) {
            return ((ZonedDateTime) javaValue)
                    .withZoneSameInstant(zoneId)
                    .format(TIMESTAMP_FORMATTER);
        }
        if (javaValue instanceof Timestamp) {
            return ((Timestamp) javaValue).toLocalDateTime().format(TIMESTAMP_FORMATTER);
        }
        return javaValue.toString();
    }
}
