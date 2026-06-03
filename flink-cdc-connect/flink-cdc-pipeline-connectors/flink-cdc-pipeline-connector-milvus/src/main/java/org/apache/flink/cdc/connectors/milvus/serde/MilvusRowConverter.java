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

package org.apache.flink.cdc.connectors.milvus.serde;

import org.apache.flink.cdc.common.converter.JavaObjectConverter;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusNameUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusTypeUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converts Flink CDC records to Milvus JSON rows. */
public class MilvusRowConverter {

    private static final Gson GSON = new Gson();

    private final Schema schema;
    private final List<MilvusVectorFieldSpec> vectorFields;
    private final int varcharMaxLengthDefault;
    private final List<RecordData.FieldGetter> fieldGetters;
    private final Map<String, MilvusVectorFieldSpec> vectorFieldMap;

    public MilvusRowConverter(
            Schema schema, List<MilvusVectorFieldSpec> vectorFields, int varcharMaxLengthDefault) {
        this.schema = schema;
        this.vectorFields = vectorFields;
        this.varcharMaxLengthDefault = varcharMaxLengthDefault;
        this.fieldGetters = SchemaUtils.createFieldGetters(schema);
        this.vectorFieldMap = new HashMap<>();
        for (MilvusVectorFieldSpec vectorField : vectorFields) {
            this.vectorFieldMap.put(vectorField.getFieldName(), vectorField);
        }
    }

    public JsonObject convert(RecordData recordData) {
        JsonObject row = new JsonObject();
        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (!column.isPhysical()) {
                continue;
            }
            Object rawValue = fieldGetters.get(i).getFieldOrNull(recordData);
            MilvusVectorFieldSpec vectorField = vectorFieldMap.get(column.getName());
            if (vectorField != null) {
                addVector(row, column.getName(), rawValue, column, vectorField);
            } else {
                addScalar(row, column, rawValue);
            }
        }
        return row;
    }

    public Object extractPrimaryKey(RecordData recordData, String primaryKey) {
        int columnIndex = schema.getColumnNames().indexOf(primaryKey);
        if (columnIndex < 0) {
            throw new IllegalStateException("Primary key column " + primaryKey + " is absent.");
        }
        Object rawValue = fieldGetters.get(columnIndex).getFieldOrNull(recordData);
        if (rawValue == null) {
            throw new IllegalStateException(
                    "Primary key column " + primaryKey + " must not be null.");
        }
        Column column = schema.getColumns().get(columnIndex);
        Object value = JavaObjectConverter.convertToJava(rawValue, column.getType());
        switch (column.getType().getTypeRoot()) {
            case BIGINT:
                return value;
            case CHAR:
            case VARCHAR:
                return value.toString();
            default:
                throw new IllegalStateException(
                        "Unsupported Milvus primary key type: "
                                + column.getType().asSummaryString());
        }
    }

    public String extractPartitionName(RecordData recordData, String partitionField) {
        if (partitionField == null || partitionField.trim().isEmpty()) {
            return null;
        }
        String field = partitionField.trim();
        int columnIndex = schema.getColumnNames().indexOf(field);
        if (columnIndex < 0) {
            throw new IllegalStateException("Partition field " + field + " is absent.");
        }
        Object rawValue = fieldGetters.get(columnIndex).getFieldOrNull(recordData);
        if (rawValue == null) {
            return null;
        }
        Column column = schema.getColumns().get(columnIndex);
        Object value = JavaObjectConverter.convertToJava(rawValue, column.getType());
        return MilvusNameUtils.normalizePartitionName(value.toString());
    }

    public List<MilvusVectorFieldSpec> getVectorFields() {
        return vectorFields;
    }

    private void addVector(
            JsonObject row,
            String columnName,
            Object rawValue,
            Column column,
            MilvusVectorFieldSpec vectorField) {
        Object javaValue = JavaObjectConverter.convertToJava(rawValue, column.getType());
        List<Float> vector = MilvusVectorConverter.toFloatVector(javaValue, vectorField);
        JsonArray array = new JsonArray();
        for (Float value : vector) {
            array.add(value);
        }
        row.add(columnName, array);
    }

    private void addScalar(JsonObject row, Column column, Object rawValue) {
        if (rawValue == null) {
            row.add(column.getName(), JsonNull.INSTANCE);
            return;
        }
        Object javaValue = JavaObjectConverter.convertToJava(rawValue, column.getType());
        switch (MilvusTypeUtils.toMilvusType(column.getType())) {
            case Bool:
                row.addProperty(column.getName(), (Boolean) javaValue);
                break;
            case Int32:
                row.addProperty(column.getName(), ((Number) javaValue).intValue());
                break;
            case Int64:
                row.addProperty(column.getName(), ((Number) javaValue).longValue());
                break;
            case Float:
                row.addProperty(column.getName(), ((Number) javaValue).floatValue());
                break;
            case Double:
                row.addProperty(column.getName(), ((Number) javaValue).doubleValue());
                break;
            case VarChar:
                row.addProperty(column.getName(), stringifyVarChar(column, javaValue));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Milvus scalar type for column " + column.getName());
        }
    }

    private String stringifyVarChar(Column column, Object value) {
        String stringValue = stringify(value);
        int maxLength =
                MilvusTypeUtils.resolveVarcharMaxLength(column.getType(), varcharMaxLengthDefault);
        if (stringValue.length() > maxLength) {
            throw new IllegalArgumentException(
                    "VarChar field "
                            + column.getName()
                            + " length "
                            + stringValue.length()
                            + " exceeds max length "
                            + maxLength
                            + ".");
        }
        return stringValue;
    }

    private String stringify(Object value) {
        if (value instanceof LocalDate
                || value instanceof LocalTime
                || value instanceof LocalDateTime
                || value instanceof Instant
                || value instanceof ZonedDateTime) {
            return value.toString();
        }
        if (value instanceof Map || value instanceof List) {
            return GSON.toJson(value);
        }
        return value.toString();
    }
}
