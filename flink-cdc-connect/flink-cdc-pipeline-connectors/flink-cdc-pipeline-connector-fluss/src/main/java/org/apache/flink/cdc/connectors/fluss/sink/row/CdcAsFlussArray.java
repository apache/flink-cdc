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

package org.apache.flink.cdc.connectors.fluss.sink.row;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.TimestampData;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import static org.apache.flink.cdc.connectors.fluss.sink.row.CdcAsFlussRow.fromFlinkDecimal;

/** Wraps a CDC {@link ArrayData} as a Fluss {@link InternalArray}. */
public class CdcAsFlussArray implements InternalArray {

    private final ArrayData flussArray;

    public CdcAsFlussArray(ArrayData flussArray) {
        this.flussArray = flussArray;
    }

    @Override
    public int size() {
        return flussArray.size();
    }

    @Override
    public boolean[] toBooleanArray() {
        return flussArray.toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return flussArray.toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return flussArray.toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return flussArray.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return flussArray.toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return flussArray.toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return flussArray.toDoubleArray();
    }

    @Override
    public boolean isNullAt(int pos) {
        return flussArray.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return flussArray.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return flussArray.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return flussArray.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return flussArray.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return flussArray.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return flussArray.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return flussArray.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(flussArray.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(flussArray.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(flussArray.getDecimal(pos, precision, scale));
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        TimestampData timestamp = flussArray.getTimestamp(pos, precision);
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        TimestampData timestamp = flussArray.getTimestamp(pos, precision);
        return TimestampLtz.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return flussArray.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return flussArray.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new CdcAsFlussArray(flussArray.getArray(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new CdcAsFlussMap(flussArray.getMap(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return CdcAsFlussRow.replace(flussArray.getRecord(pos, numFields));
    }
}
