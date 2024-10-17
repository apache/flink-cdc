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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getFieldCount;

/** A helper class for {@link PaimonWriter} to create FieldGetter and GenericRow. */
public class PaimonWriterHelper {

    /** create a list of {@link RecordData.FieldGetter} for {@link PaimonWriter}. */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        List<Column> columns = schema.getColumns();
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters.add(createFieldGetter(columns.get(i).getType(), i, zoneId));
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
                fieldGetter = row -> BinaryString.fromString(row.getString(fieldPos).toString());
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = DataTypeChecks.getPrecision(fieldType);
                final int decimalScale = DataTypeChecks.getScale(fieldType);
                fieldGetter =
                        row -> {
                            DecimalData decimalData =
                                    row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                            return Decimal.fromBigDecimal(
                                    decimalData.toBigDecimal(), decimalPrecision, decimalScale);
                        };
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        row ->
                                Timestamp.fromSQLTimestamp(
                                        row.getTimestamp(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toTimestamp());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                fieldGetter =
                        row ->
                                Timestamp.fromInstant(
                                        row.getLocalZonedTimestampData(
                                                        fieldPos,
                                                        DataTypeChecks.getPrecision(fieldType))
                                                .toInstant());
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
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

    /** create a {@link GenericRow} from a {@link DataChangeEvent} for {@link PaimonWriter}. */
    public static GenericRow convertEventToGenericRow(
            DataChangeEvent dataChangeEvent, List<RecordData.FieldGetter> fieldGetters) {
        GenericRow genericRow;
        RecordData recordData;
        switch (dataChangeEvent.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                {
                    recordData = dataChangeEvent.after();
                    genericRow = new GenericRow(RowKind.INSERT, recordData.getArity());
                    break;
                }
            case DELETE:
                {
                    recordData = dataChangeEvent.before();
                    genericRow = new GenericRow(RowKind.DELETE, recordData.getArity());
                    break;
                }
            default:
                throw new IllegalArgumentException("don't support type of " + dataChangeEvent.op());
        }
        for (int i = 0; i < recordData.getArity(); i++) {
            genericRow.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
        }
        return genericRow;
    }
}
