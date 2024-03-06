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

package com.ververica.cdc.connectors.paimon.sink.v2;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.ververica.cdc.common.types.DataTypeChecks.getPrecision;
import static com.ververica.cdc.common.types.DataTypeChecks.getScale;

/** A helper class for {@link PaimonWriter} to create FieldGetter and GenericRow. */
public class PaimonWriterHelper {

    /** create a list of {@link RecordData.FieldGetter} for {@link PaimonWriter}. */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
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
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
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
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> row.getInt(fieldPos);
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
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        row ->
                                Timestamp.fromSQLTimestamp(
                                        row.getTimestamp(fieldPos, getPrecision(fieldType))
                                                .toTimestamp());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                fieldGetter =
                        row ->
                                Timestamp.fromLocalDateTime(
                                        ZonedDateTime.ofInstant(
                                                        row.getLocalZonedTimestampData(
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

    /**
     * create a {@link GenericRow} from a {@link DataChangeEvent} for {@link
     * com.ververica.cdc.connectors.paimon.sink.v2.PaimonWriter}.
     */
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
