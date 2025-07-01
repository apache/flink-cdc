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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import java.util.Objects;

/** Wraps a CDC {@link RecordData} as a Fluss {@link InternalRow}. */
public class CdcAsFlussRow implements InternalRow {

    private final RecordData cdcRecord;

    private CdcAsFlussRow(RecordData cdcRecord) {
        this.cdcRecord = cdcRecord;
    }

    public static CdcAsFlussRow replace(RecordData cdcRecord) {
        return new CdcAsFlussRow(cdcRecord);
    }

    @Override
    public int getFieldCount() {
        return cdcRecord.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return cdcRecord.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return cdcRecord.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return cdcRecord.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return cdcRecord.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return cdcRecord.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return cdcRecord.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return cdcRecord.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return cdcRecord.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(cdcRecord.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(cdcRecord.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(cdcRecord.getDecimal(pos, precision, scale));
    }

    public static Decimal fromFlinkDecimal(DecimalData decimal) {
        if (decimal.isCompact()) {
            return Decimal.fromUnscaledLong(
                    decimal.toUnscaledLong(), decimal.precision(), decimal.scale());
        } else {
            return Decimal.fromBigDecimal(
                    decimal.toBigDecimal(), decimal.precision(), decimal.scale());
        }
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        TimestampData timestamp = cdcRecord.getTimestamp(pos, precision);
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        // Fluss ltz maybe mapping from CDC ltz or
        LocalZonedTimestampData timestamp = cdcRecord.getLocalZonedTimestampData(pos, precision);
        return TimestampLtz.fromInstant(timestamp.toInstant());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return cdcRecord.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return cdcRecord.getBinary(pos);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CdcAsFlussRow that = (CdcAsFlussRow) o;
        return Objects.equals(cdcRecord, that.cdcRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cdcRecord);
    }
}
