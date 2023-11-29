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

package com.ververica.cdc.runtime.serializer.data.writer;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.MapData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.ZonedTimestampData;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.runtime.serializer.data.ArrayDataSerializer;

/**
 * Writer to write a composite data format, like row, array. 1. Invoke {@link #reset()}. 2. Write
 * each field by writeXX or setNullAt. (Same field can not be written repeatedly.) 3. Invoke {@link
 * #complete()}.
 */
@Internal
public interface BinaryWriter {

    /** Reset writer to prepare next write. */
    void reset();

    /** Set null to this field. */
    void setNullAt(int pos);

    void writeBoolean(int pos, boolean value);

    void writeByte(int pos, byte value);

    void writeShort(int pos, short value);

    void writeInt(int pos, int value);

    void writeLong(int pos, long value);

    void writeFloat(int pos, float value);

    void writeDouble(int pos, double value);

    void writeString(int pos, StringData value);

    void writeBinary(int pos, byte[] bytes);

    void writeDecimal(int pos, DecimalData value, int precision);

    void writeTimestamp(int pos, TimestampData value, int precision);

    void writeLocalZonedTimestamp(int pos, LocalZonedTimestampData value, int precision);

    void writeZonedTimestamp(int pos, ZonedTimestampData value, int precision);

    void writeArray(int pos, ArrayData value, ArrayDataSerializer serializer);

    void writeMap(int pos, MapData value, TypeSerializer<MapData> serializer);

    void writeRecord(int pos, RecordData value, TypeSerializer<RecordData> serializer);

    /** Finally, complete write to set real size to binary. */
    void complete();

    static void write(
            BinaryWriter writer, int pos, Object o, DataType type, TypeSerializer<?> serializer) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                writer.writeBoolean(pos, (boolean) o);
                break;
            case TINYINT:
                writer.writeByte(pos, (byte) o);
                break;
            case SMALLINT:
                writer.writeShort(pos, (short) o);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                writer.writeInt(pos, (int) o);
                break;
            case BIGINT:
                writer.writeLong(pos, (long) o);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                writer.writeTimestamp(pos, (TimestampData) o, timestampType.getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
                writer.writeLocalZonedTimestamp(
                        pos, (LocalZonedTimestampData) o, lzTs.getPrecision());
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                ZonedTimestampType zTs = (ZonedTimestampType) type;
                writer.writeZonedTimestamp(pos, (ZonedTimestampData) o, zTs.getPrecision());
                break;
            case FLOAT:
                writer.writeFloat(pos, (float) o);
                break;
            case DOUBLE:
                writer.writeDouble(pos, (double) o);
                break;
            case CHAR:
            case VARCHAR:
                writer.writeString(pos, (StringData) o);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                writer.writeDecimal(pos, (DecimalData) o, decimalType.getPrecision());
                break;
            case ARRAY:
                writer.writeArray(pos, (ArrayData) o, (ArrayDataSerializer) serializer);
                break;
            case MAP:
                writer.writeMap(pos, (MapData) o, (TypeSerializer<MapData>) serializer);
                break;
            case ROW:
                writer.writeRecord(pos, (RecordData) o, (TypeSerializer<RecordData>) serializer);
                break;
            case BINARY:
            case VARBINARY:
                writer.writeBinary(pos, (byte[]) o);
                break;
            default:
                throw new UnsupportedOperationException("Not support type: " + type);
        }
    }
}
