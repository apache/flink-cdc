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

package org.apache.flink.cdc.runtime.serializer.data.binary;

import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinarySegmentUtils;
import org.apache.flink.cdc.runtime.serializer.data.util.DataFormatTestUtil;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.runtime.serializer.data.binary.BinaryRecordDataDataUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/** Utilities for binary data segments which heavily uses {@link MemorySegment}. */
class BinarySegmentUtilsTest {
    @Test
    void testCopy() {
        // test copy the content of the latter Seg
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegmentFactory.wrap(new byte[] {0, 2, 5});
        segments[1] = MemorySegmentFactory.wrap(new byte[] {6, 12, 15});

        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, 4, 2);
        assertThat(bytes).isEqualTo(new byte[] {12, 15});
    }

    @Test
    void testEquals() {
        // test copy the content of the latter Seg
        MemorySegment[] segments1 = new MemorySegment[3];
        segments1[0] = MemorySegmentFactory.wrap(new byte[] {0, 2, 5});
        segments1[1] = MemorySegmentFactory.wrap(new byte[] {6, 12, 15});
        segments1[2] = MemorySegmentFactory.wrap(new byte[] {1, 1, 1});

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegmentFactory.wrap(new byte[] {6, 0, 2, 5});
        segments2[1] = MemorySegmentFactory.wrap(new byte[] {6, 12, 15, 18});

        assertThat(BinarySegmentUtils.equalsMultiSegments(segments1, 0, segments2, 0, 0)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 3)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 6)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 7)).isFalse();
    }

    @Test
    void testBoundaryByteArrayEquals() {
        byte[] bytes1 = new byte[5];
        bytes1[3] = 81;
        byte[] bytes2 = new byte[100];
        bytes2[3] = 81;
        bytes2[4] = 81;

        assertThat(BinaryRecordDataDataUtil.byteArrayEquals(bytes1, bytes2, 4)).isTrue();
        assertThat(BinaryRecordDataDataUtil.byteArrayEquals(bytes1, bytes2, 5)).isFalse();
        assertThat(BinaryRecordDataDataUtil.byteArrayEquals(bytes1, bytes2, 0)).isTrue();
    }

    @Test
    void testBoundaryEquals() {
        BinaryRecordData row24 = DataFormatTestUtil.get24BytesBinaryRow();
        BinaryRecordData row160 = DataFormatTestUtil.get160BytesBinaryRow();
        BinaryRecordData varRow160 = DataFormatTestUtil.getMultiSeg160BytesBinaryRow(row160);
        BinaryRecordData varRow160InOne = DataFormatTestUtil.getMultiSeg160BytesInOneSegRow(row160);

        assertThat(varRow160InOne).isEqualTo(row160).isEqualTo(varRow160);
        assertThat(varRow160).isEqualTo(row160).isEqualTo(varRow160InOne);

        assertThat(row160).isNotEqualTo(row24);
        assertThat(varRow160).isNotEqualTo(row24);
        assertThat(varRow160InOne).isNotEqualTo(row24);

        assertThat(BinarySegmentUtils.equals(row24.getSegments(), 0, row160.getSegments(), 0, 0))
                .isTrue();
        assertThat(BinarySegmentUtils.equals(row24.getSegments(), 0, varRow160.getSegments(), 0, 0))
                .isTrue();

        // test var segs
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[1] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[2] = MemorySegmentFactory.wrap(new byte[16]);

        segments1[0].put(9, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14)).isFalse();
        segments2[1].put(7, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 14)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 16)).isTrue();

        segments2[2].put(7, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 32, 14)).isTrue();
    }

    @Test
    void testBoundaryCopy() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[0].put(15, (byte) 5);
        segments1[1].put(15, (byte) 6);

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 0, bytes, 0, 64);
            assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 0, 64)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 32, bytes, 0, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 32, segments2, 0, 14)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 34, bytes, 0, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 34, segments2, 0, 14)).isTrue();
        }
    }

    @Test
    void testCopyToUnsafe() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[0].put(15, (byte) 5);
        segments1[1].put(15, (byte) 6);

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToUnsafe(segments1, 0, bytes, BYTE_ARRAY_BASE_OFFSET, 64);
            assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 0, 64)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToUnsafe(segments1, 32, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 32, segments2, 0, 14)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToUnsafe(segments1, 34, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 34, segments2, 0, 14)).isTrue();
        }
    }

    @Test
    void testFind() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[1] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[2] = MemorySegmentFactory.wrap(new byte[16]);

        assertThat(BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 0)).isEqualTo(34);
        assertThat(BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 15)).isEqualTo(-1);
    }
}
