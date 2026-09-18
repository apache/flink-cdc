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

package org.apache.flink.cdc.connectors.tidb.table.utils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.tikv.common.key.Key;
import org.tikv.common.key.RowKey;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Tests for region-based key range assignment. */
class TableKeyRangeUtilsTest {

    private static final long TABLE_ID = 100L;

    @Test
    void testAssignRegionRangesBalancesByRegionCount() {
        KeyRange tableRange = TableKeyRangeUtils.getTableKeyRange(TABLE_ID);
        List<KeyRange> regionRanges = recordRanges(0, 8);

        List<KeyRange> assigned =
                TableKeyRangeUtils.assignRegionRanges(regionRanges, tableRange, 4);
        Assertions.assertThat(assigned).hasSize(4);

        // Each subtask should get 2 regions worth of handle span.
        for (int i = 0; i < 4; i++) {
            KeyRange range = assigned.get(i);
            long expectedStart = 1_000_000L * (i * 2);
            long expectedEnd = 1_000_000L * (i * 2 + 2);
            Assertions.assertThat(range.getStart())
                    .isEqualTo(RowKey.toRowKey(TABLE_ID, expectedStart).toByteString());
            Assertions.assertThat(range.getEnd())
                    .isEqualTo(RowKey.toRowKey(TABLE_ID, expectedEnd).toByteString());
        }
        assertContiguousNonOverlapping(assigned);
    }

    @Test
    void testAssignRegionRangesSortsUnsortedInput() {
        KeyRange tableRange = TableKeyRangeUtils.getTableKeyRange(TABLE_ID);
        // HashMap-like insertion order: not sorted by start key.
        List<KeyRange> unsorted =
                Arrays.asList(
                        recordRange(6_000_000L, 7_000_000L),
                        recordRange(1_000_000L, 2_000_000L),
                        recordRange(4_000_000L, 5_000_000L),
                        recordRange(2_000_000L, 3_000_000L));

        List<KeyRange> assigned = TableKeyRangeUtils.assignRegionRanges(unsorted, tableRange, 2);
        Assertions.assertThat(assigned).hasSize(2);
        Assertions.assertThat(assigned.get(0).getStart())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 1_000_000L).toByteString());
        Assertions.assertThat(assigned.get(0).getEnd())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 3_000_000L).toByteString());
        Assertions.assertThat(assigned.get(1).getStart())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 4_000_000L).toByteString());
        Assertions.assertThat(assigned.get(1).getEnd())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 7_000_000L).toByteString());
        assertContiguousNonOverlapping(assigned);
    }

    @Test
    void testAssignRegionRangesClipsToTableRange() {
        KeyRange tableRange =
                KeyRangeUtils.makeCoprocRange(
                        RowKey.toRowKey(TABLE_ID, 100).toByteString(),
                        RowKey.toRowKey(TABLE_ID, 200).toByteString());
        List<KeyRange> regionRanges =
                Arrays.asList(recordRange(0, 150), recordRange(150, 300), recordRange(300, 400));

        List<KeyRange> assigned =
                TableKeyRangeUtils.assignRegionRanges(regionRanges, tableRange, 2);
        Assertions.assertThat(assigned).hasSize(2);
        Assertions.assertThat(assigned.get(0).getStart())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 100).toByteString());
        Assertions.assertThat(assigned.get(0).getEnd())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 150).toByteString());
        Assertions.assertThat(assigned.get(1).getStart())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 150).toByteString());
        Assertions.assertThat(assigned.get(1).getEnd())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 200).toByteString());
        assertContiguousNonOverlapping(assigned);
    }

    @Test
    void testAssignRegionRangesWhenParallelismExceedsRegions() {
        KeyRange tableRange = TableKeyRangeUtils.getTableKeyRange(TABLE_ID);
        List<KeyRange> regionRanges = new ArrayList<>();
        regionRanges.add(recordRange(1, 100));
        regionRanges.add(recordRange(100, 200));

        List<KeyRange> assigned =
                TableKeyRangeUtils.assignRegionRanges(regionRanges, tableRange, 4);
        Assertions.assertThat(assigned).hasSize(4);

        // First two subtasks each get one region; the rest get empty ranges.
        Assertions.assertThat(assigned.get(0).getStart())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 1).toByteString());
        Assertions.assertThat(assigned.get(0).getEnd())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 100).toByteString());
        Assertions.assertThat(assigned.get(1).getStart())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 100).toByteString());
        Assertions.assertThat(assigned.get(1).getEnd())
                .isEqualTo(RowKey.toRowKey(TABLE_ID, 200).toByteString());
        Assertions.assertThat(isEmptyRange(assigned.get(2))).isTrue();
        Assertions.assertThat(isEmptyRange(assigned.get(3))).isTrue();
    }

    @Test
    void testIsRecordKey() {
        ByteString record = RowKey.toRowKey(TABLE_ID, 42L).toByteString();
        Assertions.assertThat(TableKeyRangeUtils.isRecordKey(record.toByteArray())).isTrue();
    }

    private static List<KeyRange> recordRanges(int fromInclusive, int count) {
        List<KeyRange> regionRanges = new ArrayList<>();
        for (int i = fromInclusive; i < fromInclusive + count; i++) {
            regionRanges.add(recordRange(1_000_000L * i, 1_000_000L * (i + 1)));
        }
        return regionRanges;
    }

    private static KeyRange recordRange(long startHandle, long endHandle) {
        return KeyRangeUtils.makeCoprocRange(
                RowKey.toRowKey(TABLE_ID, startHandle).toByteString(),
                RowKey.toRowKey(TABLE_ID, endHandle).toByteString());
    }

    private static boolean isEmptyRange(KeyRange range) {
        return range.getStart().equals(range.getEnd());
    }

    private static void assertContiguousNonOverlapping(List<KeyRange> ranges) {
        for (int i = 1; i < ranges.size(); i++) {
            if (isEmptyRange(ranges.get(i - 1)) || isEmptyRange(ranges.get(i))) {
                continue;
            }
            Key prevEnd = Key.toRawKey(ranges.get(i - 1).getEnd());
            Key curStart = Key.toRawKey(ranges.get(i).getStart());
            Assertions.assertThat(prevEnd.compareTo(curStart)).isLessThanOrEqualTo(0);
        }
    }
}
