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

package org.apache.flink.cdc.runtime.serializer.data.writer;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinarySegmentUtils;
import org.apache.flink.core.memory.MemorySegmentFactory;

/** Writer for {@link BinaryRecordData}. */
@Internal
public final class BinaryRecordDataWriter extends AbstractBinaryWriter {

    private final int nullBitsSizeInBytes;
    private final BinaryRecordData row;
    private final int fixedSize;

    public BinaryRecordDataWriter(BinaryRecordData row) {
        this(row, 0);
    }

    public BinaryRecordDataWriter(BinaryRecordData row, int initialSize) {
        this.nullBitsSizeInBytes = BinaryRecordData.calculateBitSetWidthInBytes(row.getArity());
        this.fixedSize = row.getFixedLengthPartSize();
        this.cursor = fixedSize;

        this.segment = MemorySegmentFactory.wrap(new byte[fixedSize + initialSize]);
        this.row = row;
        this.row.pointTo(segment, 0, segment.size());
    }

    /** First, reset. */
    @Override
    public void reset() {
        this.cursor = fixedSize;
        for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
            segment.putLong(i, 0L);
        }
    }

    /** Default not null. */
    @Override
    public void setNullAt(int pos) {
        setNullBit(pos);
        segment.putLong(getFieldOffset(pos), 0L);
    }

    @Override
    public void setNullBit(int pos) {
        BinarySegmentUtils.bitSet(segment, 0, pos + BinaryRecordData.HEADER_SIZE_IN_BITS);
    }

    @Override
    public void writeBoolean(int pos, boolean value) {
        segment.putBoolean(getFieldOffset(pos), value);
    }

    @Override
    public void writeByte(int pos, byte value) {
        segment.put(getFieldOffset(pos), value);
    }

    @Override
    public void writeShort(int pos, short value) {
        segment.putShort(getFieldOffset(pos), value);
    }

    @Override
    public void writeInt(int pos, int value) {
        segment.putInt(getFieldOffset(pos), value);
    }

    @Override
    public void writeLong(int pos, long value) {
        segment.putLong(getFieldOffset(pos), value);
    }

    @Override
    public void writeFloat(int pos, float value) {
        segment.putFloat(getFieldOffset(pos), value);
    }

    @Override
    public void writeDouble(int pos, double value) {
        segment.putDouble(getFieldOffset(pos), value);
    }

    @Override
    public void complete() {
        row.setTotalSize(cursor);
    }

    @Override
    public int getFieldOffset(int pos) {
        return nullBitsSizeInBytes + 8 * pos;
    }

    @Override
    public void setOffsetAndSize(int pos, int offset, long size) {
        final long offsetAndSize = ((long) offset << 32) | size;
        segment.putLong(getFieldOffset(pos), offsetAndSize);
    }

    @Override
    public void afterGrow() {
        row.pointTo(segment, 0, segment.size());
    }
}
