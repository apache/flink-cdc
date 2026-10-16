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

package org.apache.flink.cdc.common.data.binary;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the reader-side migration of pre-upgrade {@code TIME} array payloads.
 *
 * <p>Before high-precision {@code TIME} support, every {@code TIME} element of a {@link
 * BinaryArrayData} occupied a 4-byte millisecond slot. The current layout always uses an 8-byte
 * slot holding {@code Long.MIN_VALUE | nanoOfDay}, independently of the element precision. A
 * payload written by the old code therefore has a different element stride, and {@link
 * BinaryArrayData} must recover the millisecond value by judging the slot width from {@code
 * sizeInBytes} itself.
 */
class BinaryArrayDataTest {

    private static final int MILLIS_TO_NANO = 1_000_000;

    @Test
    void readsPreUpgradeFourByteTimeArrayPayload() {
        long[] millis = {1000L, 2000L, 3_723_123L};
        byte[] bytes = new byte[32];

        BinaryArrayData array = pointToLegacyMillisArray(bytes, millis);

        assertThat(array.size()).isEqualTo(millis.length);
        assertThat(array.getSizeInBytes()).isEqualTo(24);
        for (int i = 0; i < millis.length; i++) {
            assertThat(array.getTime(i).toNanoOfDay()).isEqualTo(millis[i] * MILLIS_TO_NANO);
        }
    }

    @Test
    void readsUpgradedEightByteTimeArrayPayload() {
        long[] nanos = {3_723_123_456_789L, 1L};
        MemorySegment segment = MemorySegmentFactory.wrap(new byte[24]);
        segment.putInt(0, nanos.length);
        segment.putInt(4, 0);
        for (int i = 0; i < nanos.length; i++) {
            segment.putLong(8 + i * 8, Long.MIN_VALUE | nanos[i]);
        }

        BinaryArrayData array = new BinaryArrayData();
        array.pointTo(segment, 0, 24);

        assertThat(array.getSizeInBytes()).isEqualTo(24);
        for (int i = 0; i < nanos.length; i++) {
            assertThat(array.getTime(i).toNanoOfDay()).isEqualTo(nanos[i]);
        }
    }

    @Test
    void readsSingleElementLegacyTimeArrayThroughTheSlotTag() {
        // A one-element legacy array occupies the same rounded payload length as the current
        // layout, so the element stride cannot be derived from sizeInBytes. The reader must fall
        // back to the tag bit of the slot: the millisecond layout leaves it clear.
        byte[] bytes = new byte[16];
        MemorySegment segment = MemorySegmentFactory.wrap(bytes);
        segment.putInt(0, 1);
        segment.putInt(8, 3_723_123);

        BinaryArrayData array = new BinaryArrayData();
        array.pointTo(segment, 0, 16);

        assertThat(array.getTime(0).toNanoOfDay()).isEqualTo(3_723_123L * MILLIS_TO_NANO);
    }

    @Test
    void legacyTimeArrayInsideRecordKeepsSiblingFieldsIntact() {
        long[] millis = {1000L, 2000L, 3_723_123L};
        byte[] arrayBlob = legacyMillisArrayBytes(millis);
        byte[] rowBytes = new byte[24 + arrayBlob.length];

        // Field 0 is an INTEGER, field 1 is the offset&size slot of the nested array.
        MemorySegment segment = MemorySegmentFactory.wrap(rowBytes);
        segment.putInt(8, 42);
        segment.putLong(16, ((long) 24 << 32) | arrayBlob.length);
        segment.put(24, arrayBlob, 0, arrayBlob.length);

        BinaryRecordData row = new BinaryRecordData(2);
        row.pointTo(segment, 0, rowBytes.length);

        assertThat(row.getInt(0)).isEqualTo(42);
        ArrayData nested = row.getArray(1);
        assertThat(nested.size()).isEqualTo(millis.length);
        for (int i = 0; i < millis.length; i++) {
            assertThat(nested.getTime(i).toNanoOfDay()).isEqualTo(millis[i] * MILLIS_TO_NANO);
        }
    }

    @Test
    void legacyTimeArrayInsideMapValueArrayIsRead() {
        long[] millis = {1000L, 2_000L};
        byte[] valueBlob = legacyMillisArrayBytes(millis);

        BinaryArrayData keys = BinaryArrayData.fromPrimitiveArray(new int[] {10, 20});
        byte[] keyBlob =
                BinarySegmentUtils.copyToBytes(
                        keys.getSegments(), keys.getOffset(), keys.getSizeInBytes());

        byte[] mapBytes = new byte[4 + keyBlob.length + valueBlob.length];
        MemorySegment segment = MemorySegmentFactory.wrap(mapBytes);
        segment.putInt(0, keyBlob.length);
        segment.put(4, keyBlob, 0, keyBlob.length);
        segment.put(4 + keyBlob.length, valueBlob, 0, valueBlob.length);

        BinaryMapData map = new BinaryMapData();
        map.pointTo(segment, 0, mapBytes.length);

        assertThat(map.size()).isEqualTo(millis.length);
        assertThat(map.keyArray().getInt(0)).isEqualTo(10);
        assertThat(map.keyArray().getInt(1)).isEqualTo(20);
        for (int i = 0; i < millis.length; i++) {
            assertThat(map.valueArray().getTime(i).toNanoOfDay())
                    .isEqualTo(millis[i] * MILLIS_TO_NANO);
        }
    }

    @Test
    void nullElementsInLegacyTimeArrayDecodeAsNull() {
        byte[] bytes = new byte[32];
        MemorySegment segment = MemorySegmentFactory.wrap(bytes);
        segment.putInt(0, 2);
        // Old setNullInt semantics: the null bit is set and the 4-byte slot is zeroed.
        segment.putInt(4, 1);
        segment.putInt(8, 0);
        segment.putInt(12, 4_000);

        BinaryArrayData array = new BinaryArrayData();
        array.pointTo(segment, 0, 16);

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.isNullAt(1)).isFalse();
        assertThat(array.getTime(1).toNanoOfDay()).isEqualTo(4_000L * MILLIS_TO_NANO);
    }

    @Test
    void nullElementsInCurrentTimeArrayDecodeAsNull() {
        MemorySegment segment = MemorySegmentFactory.wrap(new byte[24]);
        segment.putInt(0, 2);
        segment.putInt(4, 1);
        segment.putLong(8, 0L);
        segment.putLong(16, Long.MIN_VALUE | 4_000_123_456L);

        BinaryArrayData array = new BinaryArrayData();
        array.pointTo(segment, 0, 24);

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.getTime(1).toNanoOfDay()).isEqualTo(4_000_123_456L);
    }

    @Test
    void repointingResetsLegacyLayoutDetection() {
        long[] millis = {1000L, 2000L, 3_723_123L};
        BinaryArrayData array = pointToLegacyMillisArray(new byte[32], millis);
        assertThat(array.getTime(0).toNanoOfDay()).isEqualTo(millis[0] * MILLIS_TO_NANO);

        long nanos = 3_723_123_456_789L;
        MemorySegment current = MemorySegmentFactory.wrap(new byte[16]);
        current.putInt(0, 1);
        current.putLong(8, Long.MIN_VALUE | nanos);
        array.pointTo(current, 0, 16);

        assertThat(array.getTime(0).toNanoOfDay()).isEqualTo(nanos);
    }

    // ------------------------------------------------------------------------------------------

    /** Hand-writes a pre-upgrade {@code [size][nullbits][4 * N millis][pad]} payload. */
    private static BinaryArrayData pointToLegacyMillisArray(byte[] bytes, long[] millis) {
        byte[] blob = legacyMillisArrayBytes(millis);
        System.arraycopy(blob, 0, bytes, 0, blob.length);
        BinaryArrayData array = new BinaryArrayData();
        array.pointTo(MemorySegmentFactory.wrap(bytes), 0, blob.length);
        return array;
    }

    private static byte[] legacyMillisArrayBytes(long[] millis) {
        int header = BinaryArrayData.calculateHeaderInBytes(millis.length);
        int sizeInBytes = ((header + 4 * millis.length) + 7) / 8 * 8;
        byte[] blob = new byte[sizeInBytes];
        MemorySegment segment = MemorySegmentFactory.wrap(blob);
        segment.putInt(0, millis.length);
        for (int i = 0; i < millis.length; i++) {
            segment.putInt(header + i * 4, (int) millis[i]);
        }
        return blob;
    }
}
