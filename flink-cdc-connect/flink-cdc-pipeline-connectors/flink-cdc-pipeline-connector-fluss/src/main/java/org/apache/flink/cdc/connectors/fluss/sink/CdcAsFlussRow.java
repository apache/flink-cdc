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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Wraps a CDC {@link RecordData} as a Fluss {@link InternalRow}. */
@Internal
public class CdcAsFlussRow implements InternalRow {

    private final RecordData cdcRecord;

    /**
     * Index mapping from Fluss Type to Cdc Type which will be used to convert CDC type to Fluss.
     */
    private final Map<Integer, Integer> indexMapping;

    private final Integer rowFieldCount;

    private CdcAsFlussRow(
            RecordData cdcRecord, Integer rowFieldCount, Map<Integer, Integer> indexMapping) {
        this.cdcRecord = cdcRecord;
        this.rowFieldCount = rowFieldCount;
        this.indexMapping = indexMapping;
    }

    public static CdcAsFlussRow replace(
            RecordData cdcRecord, Integer rowFieldCount, Map<Integer, Integer> indexMapping) {
        return new CdcAsFlussRow(cdcRecord, rowFieldCount, indexMapping);
    }

    public static CdcAsFlussRow replace(RecordData cdcRecord) {
        return new CdcAsFlussRow(
                cdcRecord,
                cdcRecord.getArity(),
                IntStream.range(0, cdcRecord.getArity())
                        .boxed()
                        .collect(Collectors.toMap(i -> i, i -> i)));
    }

    @Override
    public int getFieldCount() {
        return rowFieldCount;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (indexMapping.get(pos) == null) {
            return true;
        }

        return cdcRecord.isNullAt(indexMapping.get(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return cdcRecord.getBoolean(indexMapping.get(pos));
    }

    @Override
    public byte getByte(int pos) {
        return cdcRecord.getByte(indexMapping.get(pos));
    }

    @Override
    public short getShort(int pos) {
        return cdcRecord.getShort(indexMapping.get(pos));
    }

    @Override
    public int getInt(int pos) {
        return cdcRecord.getInt(indexMapping.get(pos));
    }

    @Override
    public long getLong(int pos) {
        return cdcRecord.getLong(indexMapping.get(pos));
    }

    @Override
    public float getFloat(int pos) {
        return cdcRecord.getFloat(indexMapping.get(pos));
    }

    @Override
    public double getDouble(int pos) {
        return cdcRecord.getDouble(indexMapping.get(pos));
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(cdcRecord.getString(indexMapping.get(pos)).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(cdcRecord.getString(indexMapping.get(pos)).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(cdcRecord.getDecimal(indexMapping.get(pos), precision, scale));
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
        TimestampData timestamp = cdcRecord.getTimestamp(indexMapping.get(pos), precision);
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        // Fluss ltz maybe mapping from CDC ltz or
        LocalZonedTimestampData timestamp =
                cdcRecord.getLocalZonedTimestampData(indexMapping.get(pos), precision);
        return TimestampLtz.fromInstant(timestamp.toInstant());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return cdcRecord.getBinary(indexMapping.get(pos));
    }

    @Override
    public byte[] getBytes(int pos) {
        return cdcRecord.getBinary(indexMapping.get(pos));
    }

    @VisibleForTesting
    public RecordData getCdcRecord() {
        return cdcRecord;
    }

    @VisibleForTesting
    public Map<Integer, Integer> getIndexMapping() {
        return indexMapping;
    }
}
