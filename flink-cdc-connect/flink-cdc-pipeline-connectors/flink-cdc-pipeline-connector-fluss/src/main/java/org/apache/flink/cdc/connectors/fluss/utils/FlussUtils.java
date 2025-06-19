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

package org.apache.flink.cdc.connectors.fluss.utils;

import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.types.DataType;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import java.time.ZoneId;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

public class FlussUtils {

    public static RecordData.FieldGetter createFieldGetter(
            DataType fieldType, int fieldPos, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case TINYINT:
                fieldGetter = record -> record.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = record -> record.getShort(fieldPos);
                break;
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
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
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter =
                        record ->
                                Decimal.fromBigDecimal(
                                        record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                                .toBigDecimal(),
                                        decimalPrecision,
                                        decimalScale);
                break;
            case CHAR:
            case VARCHAR:
                fieldGetter =
                        record -> BinaryString.fromBytes(record.getString(fieldPos).toBytes());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldGetter =
                        record -> {
                            TimestampData timestamp =
                                    record.getTimestamp(fieldPos, getPrecision(fieldType));
                            return TimestampNtz.fromMillis(
                                    timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
                        };
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldGetter =
                        record -> {
                            LocalZonedTimestampData timestamp =
                                    record.getLocalZonedTimestampData(
                                            fieldPos, getPrecision(fieldType));
                            return TimestampLtz.fromEpochMillis(
                                    timestamp.getEpochMillisecond(),
                                    timestamp.getEpochNanoOfMillisecond());
                        };
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support data type " + fieldType.getTypeRoot());
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
}
