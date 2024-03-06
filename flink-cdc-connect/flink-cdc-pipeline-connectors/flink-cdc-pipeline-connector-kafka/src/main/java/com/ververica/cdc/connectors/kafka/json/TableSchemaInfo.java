/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.kafka.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.ververica.cdc.common.types.DataTypeChecks.getPrecision;
import static com.ververica.cdc.common.types.DataTypeChecks.getScale;

/** maintain the {@link SerializationSchema} of a specific {@link TableId}. */
public class TableSchemaInfo {

    private final Schema schema;

    private final List<RecordData.FieldGetter> fieldGetters;

    private final SerializationSchema<RowData> serializationSchema;

    public TableSchemaInfo(
            Schema schema, SerializationSchema<RowData> serializationSchema, ZoneId zoneId) {
        this.schema = schema;
        this.serializationSchema = serializationSchema;
        this.fieldGetters = createFieldGetters(schema, zoneId);
    }

    /** convert to {@link RowData}, which will be pass to serializationSchema. */
    public RowData getRowDataFromRecordData(RecordData recordData) {
        GenericRowData genericRowData = new GenericRowData(recordData.getArity());
        for (int i = 0; i < recordData.getArity(); i++) {
            genericRowData.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
        }
        return genericRowData;
    }

    private static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(schema.getColumns().size());
        for (int i = 0; i < schema.getColumns().size(); i++) {
            fieldGetters.add(createFieldGetter(schema.getColumns().get(i).getType(), i, zoneId));
        }
        return fieldGetters;
    }

    private static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter =
                        record ->
                                BinaryStringData.fromString(record.getString(fieldPos).toString());
                break;
            case BOOLEAN:
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = record -> record.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter =
                        record ->
                                DecimalData.fromBigDecimal(
                                        record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                                .toBigDecimal(),
                                        decimalPrecision,
                                        decimalScale);
                break;
            case TINYINT:
                fieldGetter = record -> record.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = record -> record.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = record -> record.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = record -> record.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = record -> record.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = record -> record.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record ->
                                TimestampData.fromTimestamp(
                                        record.getTimestamp(fieldPos, getPrecision(fieldType))
                                                .toTimestamp());
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record ->
                                TimestampData.fromLocalDateTime(
                                        ZonedDateTime.ofInstant(
                                                        record.getLocalZonedTimestampData(
                                                                        fieldPos,
                                                                        getPrecision(fieldType))
                                                                .toInstant(),
                                                        zoneId)
                                                .toLocalDateTime());
                break;
            default:
                throw new IllegalArgumentException(
                        "don't support type of " + fieldType.getTypeRoot());
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    public Schema getSchema() {
        return schema;
    }

    public SerializationSchema<RowData> getSerializationSchema() {
        return serializationSchema;
    }
}
