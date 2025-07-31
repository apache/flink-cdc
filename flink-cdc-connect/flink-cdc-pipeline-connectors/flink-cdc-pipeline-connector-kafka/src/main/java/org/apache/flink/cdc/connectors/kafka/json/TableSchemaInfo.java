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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** maintain the {@link SerializationSchema} of a specific {@link TableId}. */
public class TableSchemaInfo {

    private final TableId tableId;

    private final Schema schema;

    private final List<Integer> primaryKeyColumnIndexes;

    private final List<RecordData.FieldGetter> fieldGetters;

    private final SerializationSchema<RowData> serializationSchema;

    public TableSchemaInfo(
            TableId tableId,
            Schema schema,
            SerializationSchema<RowData> serializationSchema,
            ZoneId zoneId) {
        this.tableId = tableId;
        this.schema = schema;
        this.serializationSchema = serializationSchema;
        this.fieldGetters = createFieldGetters(schema, zoneId);
        primaryKeyColumnIndexes = new ArrayList<>();
        for (int keyIndex = 0; keyIndex < schema.primaryKeys().size(); keyIndex++) {
            for (int columnIndex = 0; columnIndex < schema.getColumnCount(); columnIndex++) {
                if (schema.getColumns()
                        .get(columnIndex)
                        .getName()
                        .equals(schema.primaryKeys().get(keyIndex))) {
                    primaryKeyColumnIndexes.add(columnIndex);
                    break;
                }
            }
        }
    }

    /** convert to {@link RowData}, which will be pass to serializationSchema. */
    public RowData getRowDataFromRecordData(RecordData recordData, boolean primaryKeyOnly) {
        if (primaryKeyOnly) {
            GenericRowData genericRowData = new GenericRowData(primaryKeyColumnIndexes.size() + 1);
            genericRowData.setField(0, StringData.fromString(tableId.toString()));
            for (int i = 0; i < primaryKeyColumnIndexes.size(); i++) {
                genericRowData.setField(
                        i + 1,
                        fieldGetters
                                .get(primaryKeyColumnIndexes.get(i))
                                .getFieldOrNull(recordData));
            }
            return genericRowData;
        } else {
            GenericRowData genericRowData = new GenericRowData(recordData.getArity());
            for (int i = 0; i < recordData.getArity(); i++) {
                genericRowData.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
            }
            return genericRowData;
        }
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
                fieldGetter = record -> record.getInt(fieldPos);
                break;
            case DATE:
                fieldGetter = record -> (int) record.getDate(fieldPos).toEpochDay();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = record -> (int) record.getTime(fieldPos).toMillisOfDay();
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
                                TimestampData.fromInstant(
                                        record.getLocalZonedTimestampData(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toInstant());
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
