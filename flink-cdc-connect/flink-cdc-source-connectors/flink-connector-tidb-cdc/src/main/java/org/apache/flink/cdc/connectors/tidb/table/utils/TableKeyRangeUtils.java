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

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.key.RowKey;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.RangeSplitter.RegionTask;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Utils to obtain the keyRange of table.
 *
 * <p>Prefer splitting by TiKV regions (balanced by region count) over evenly splitting the whole
 * {@code long} handle space, which causes severe skew for AUTO_INCREMENT primary keys.
 */
public class TableKeyRangeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableKeyRangeUtils.class);

    private static final Comparator<KeyRange> START_KEY_COMPARATOR =
            Comparator.comparing(range -> Key.toRawKey(range.getStart()));

    private TableKeyRangeUtils() {}

    public static KeyRange getTableKeyRange(final long tableId) {
        return KeyRangeUtils.makeCoprocRange(
                RowKey.createMin(tableId).toByteString(),
                RowKey.createBeyondMax(tableId).toByteString());
    }

    /**
     * Fetch regions covering the table, then partition them into {@code num} contiguous chunks
     * (balanced by region count). Each chunk is merged into one {@link KeyRange}.
     *
     * <p>Must be called from a single coordinator so every subtask observes the same region
     * snapshot.
     */
    public static List<KeyRange> getTableKeyRangesByRegion(
            final TiSession session, final long tableId, final int num) {
        Preconditions.checkArgument(num > 0, "Illegal value of num");
        Preconditions.checkNotNull(session, "TiSession must not be null");

        final KeyRange tableRange = getTableKeyRange(tableId);
        if (num == 1) {
            return ImmutableList.of(tableRange);
        }

        final List<RegionTask> regionTasks =
                RangeSplitter.newSplitter(session.getRegionManager())
                        .splitRangeByRegion(Collections.singletonList(tableRange));

        final List<KeyRange> regionRanges = new ArrayList<>();
        for (RegionTask regionTask : regionTasks) {
            List<KeyRange> ranges = regionTask.getRanges();
            if (ranges == null || ranges.isEmpty()) {
                continue;
            }
            regionRanges.addAll(ranges);
        }

        LOG.info(
                "TableId={} covers {} region range(s), splitting into {} parallel subtask(s)",
                tableId,
                regionRanges.size(),
                num);

        return assignRegionRanges(regionRanges, tableRange, num);
    }

    /**
     * Assign region key-ranges to {@code num} subtasks by contiguous chunks and merge each chunk
     * into a single key range.
     *
     * <p>Input ranges are clipped to {@code tableRange} and sorted by start key. This is required
     * because {@code RangeSplitter.splitRangeByRegion} groups tasks in a HashMap and does not
     * return key order. Merging unsorted ranges would produce overlapping scans across subtasks.
     */
    public static List<KeyRange> assignRegionRanges(
            final List<KeyRange> regionRanges, final KeyRange tableRange, final int num) {
        Preconditions.checkArgument(num > 0, "Illegal value of num");
        Preconditions.checkNotNull(regionRanges, "regionRanges must not be null");
        Preconditions.checkNotNull(tableRange, "tableRange must not be null");

        if (num == 1) {
            return ImmutableList.of(tableRange);
        }

        final List<KeyRange> sorted = normalizeRegionRanges(regionRanges, tableRange);
        if (sorted.isEmpty()) {
            List<KeyRange> empty = new ArrayList<>(num);
            empty.add(tableRange);
            for (int i = 1; i < num; i++) {
                empty.add(emptyKeyRange(tableRange));
            }
            return empty;
        }

        final ImmutableList.Builder<KeyRange> builder = ImmutableList.builder();
        final int total = sorted.size();

        // When there are fewer regions than subtasks, give one region to each of the first
        // `total` subtasks and leave the rest empty. Integer-chunking would otherwise create
        // leading empty slots (e.g. 2 regions / 4 tasks -> [empty, r0, empty, r1]).
        if (total <= num) {
            for (int i = 0; i < num; i++) {
                if (i < total) {
                    builder.add(sorted.get(i));
                    LOG.debug("Subtask {}/{} gets region range index [{}, {})", i, num, i, i + 1);
                } else {
                    builder.add(emptyKeyRange(tableRange));
                    LOG.debug("Subtask {}/{} gets empty keyRange (no region)", i, num);
                }
            }
            return builder.build();
        }

        for (int i = 0; i < num; i++) {
            final int startIdx = (int) ((long) total * i / num);
            final int endIdx = (int) ((long) total * (i + 1) / num);
            List<KeyRange> slice = new ArrayList<>(sorted.subList(startIdx, endIdx));
            List<KeyRange> merged = KeyRangeUtils.mergeSortedRanges(slice);
            builder.add(spanKeyRanges(merged));
            LOG.debug(
                    "Subtask {}/{} gets region ranges [{}, {}), merged into {} key range(s)",
                    i,
                    num,
                    startIdx,
                    endIdx,
                    merged.size());
        }
        return builder.build();
    }

    /** Clip to table range, drop empties, sort by start key. Visible for testing. */
    static List<KeyRange> normalizeRegionRanges(
            final List<KeyRange> regionRanges, final KeyRange tableRange) {
        final List<KeyRange> normalized = new ArrayList<>(regionRanges.size());
        for (KeyRange range : regionRanges) {
            KeyRange clipped = intersect(range, tableRange);
            if (clipped != null) {
                normalized.add(clipped);
            }
        }
        normalized.sort(START_KEY_COMPARATOR);
        return normalized;
    }

    /** Inclusive-start exclusive-end intersection; {@code null} if empty. */
    static KeyRange intersect(final KeyRange left, final KeyRange right) {
        final Key start = maxKey(Key.toRawKey(left.getStart()), Key.toRawKey(right.getStart()));
        final Key end = minKey(Key.toRawKey(left.getEnd()), Key.toRawKey(right.getEnd()));
        if (start.compareTo(end) >= 0) {
            return null;
        }
        return KeyRangeUtils.makeCoprocRange(start.toByteString(), end.toByteString());
    }

    private static Key maxKey(final Key a, final Key b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    private static Key minKey(final Key a, final Key b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    /** Create a zero-width key range that yields no scan results. */
    static KeyRange emptyKeyRange(final KeyRange tableRange) {
        ByteString start = tableRange.getStart();
        return KeyRangeUtils.makeCoprocRange(start, start);
    }

    /** Span a list of (usually already merged contiguous) ranges into one KeyRange. */
    static KeyRange spanKeyRanges(final List<KeyRange> ranges) {
        Preconditions.checkArgument(
                ranges != null && !ranges.isEmpty(), "ranges must not be empty");
        if (ranges.size() == 1) {
            return ranges.get(0);
        }
        return KeyRangeUtils.makeCoprocRange(
                ranges.get(0).getStart(), ranges.get(ranges.size() - 1).getEnd());
    }

    /**
     * @deprecated Uneven for AUTO_INCREMENT keys; use {@link #getTableKeyRangesByRegion(TiSession,
     *     long, int)} from a single coordinator instead.
     */
    @Deprecated
    public static List<KeyRange> getTableKeyRanges(final long tableId, final int num) {
        return getTableKeyRangesByHandle(tableId, num);
    }

    /**
     * @deprecated Uneven for AUTO_INCREMENT keys; use {@link #getTableKeyRangesByRegion(TiSession,
     *     long, int)} from a single coordinator instead.
     */
    @Deprecated
    public static KeyRange getTableKeyRange(final long tableId, final int num, final int idx) {
        return getTableKeyRangeByHandle(tableId, num, idx);
    }

    /** Legacy handle-space split kept as fallback for callers that still need it. */
    public static List<KeyRange> getTableKeyRangesByHandle(final long tableId, final int num) {
        Preconditions.checkArgument(num > 0, "Illegal value of num");

        if (num == 1) {
            return ImmutableList.of(getTableKeyRange(tableId));
        }

        final long delta =
                BigInteger.valueOf(Long.MAX_VALUE)
                        .subtract(BigInteger.valueOf(Long.MIN_VALUE + 1))
                        .divide(BigInteger.valueOf(num))
                        .longValueExact();
        final ImmutableList.Builder<KeyRange> builder = ImmutableList.builder();
        for (int i = 0; i < num; i++) {
            final RowKey startKey =
                    (i == 0)
                            ? RowKey.createMin(tableId)
                            : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * i);
            final RowKey endKey =
                    (i == num - 1)
                            ? RowKey.createBeyondMax(tableId)
                            : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * (i + 1));
            builder.add(
                    KeyRangeUtils.makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
        }
        return builder.build();
    }

    public static KeyRange getTableKeyRangeByHandle(
            final long tableId, final int num, final int idx) {
        Preconditions.checkArgument(idx >= 0 && idx < num, "Illegal value of idx");
        return getTableKeyRangesByHandle(tableId, num).get(idx);
    }

    public static boolean isRecordKey(final byte[] key) {
        return key[9] == '_' && key[10] == 'r';
    }
}
