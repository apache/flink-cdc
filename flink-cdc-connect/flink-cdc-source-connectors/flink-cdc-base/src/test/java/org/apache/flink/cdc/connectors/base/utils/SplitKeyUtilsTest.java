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

package org.apache.flink.cdc.connectors.base.utils;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.connectors.base.utils.SplitKeyUtils.findSplitByKeyBinary;
import static org.apache.flink.cdc.connectors.base.utils.SplitKeyUtils.sortFinishedSplitInfos;
import static org.apache.flink.cdc.connectors.base.utils.SplitKeyUtils.splitKeyRangeContains;

/** Tests for {@link SplitKeyUtils}. */
class SplitKeyUtilsTest {

    @Test
    void testSortFinishedSplitInfos() {
        TableId tableId = TableId.parse("test_db.test_table");
        List<FinishedSnapshotSplitInfo> splits = new ArrayList<>();
        splits.add(createSplit(tableId, "split-2", new Object[] {200L}, null));
        splits.add(createSplit(tableId, "split-0", null, new Object[] {100L}));
        splits.add(createSplit(tableId, "split-1", new Object[] {100L}, new Object[] {200L}));

        sortFinishedSplitInfos(splits);

        Assertions.assertThat(splits)
                .extracting(FinishedSnapshotSplitInfo::getSplitId)
                .containsExactly("split-0", "split-1", "split-2");
    }

    @Test
    void testFindSplitByKeyBinaryAcrossMultipleSplits() {
        TableId tableId = TableId.parse("test_db.test_table");
        List<FinishedSnapshotSplitInfo> splits = new ArrayList<>();
        splits.add(createSplit(tableId, "split-0", null, new Object[] {100L}));
        splits.add(createSplit(tableId, "split-1", new Object[] {100L}, new Object[] {200L}));
        splits.add(createSplit(tableId, "split-2", new Object[] {200L}, new Object[] {300L}));
        splits.add(createSplit(tableId, "split-3", new Object[] {300L}, null));

        sortFinishedSplitInfos(splits);

        Assertions.assertThat(findSplitByKeyBinary(splits, new Object[] {50L}).getSplitId())
                .isEqualTo("split-0");
        Assertions.assertThat(findSplitByKeyBinary(splits, new Object[] {150L}).getSplitId())
                .isEqualTo("split-1");
        Assertions.assertThat(findSplitByKeyBinary(splits, new Object[] {250L}).getSplitId())
                .isEqualTo("split-2");
        Assertions.assertThat(
                        findSplitByKeyBinary(splits, new Object[] {Long.MAX_VALUE}).getSplitId())
                .isEqualTo("split-3");
    }

    @Test
    void testFindSplitByKeyBinaryEdgeCases() {
        TableId tableId = TableId.parse("test_db.test_table");
        List<FinishedSnapshotSplitInfo> splits = new ArrayList<>();
        splits.add(createSplit(tableId, "split-0", null, new Object[] {100L}));

        sortFinishedSplitInfos(splits);

        Assertions.assertThat(findSplitByKeyBinary(splits, new Object[] {50L}).getSplitId())
                .isEqualTo("split-0");
        Assertions.assertThat(findSplitByKeyBinary(splits, new Object[] {100L})).isNull();

        Assertions.assertThat(findSplitByKeyBinary(new ArrayList<>(), new Object[] {1L})).isNull();
        Assertions.assertThat(findSplitByKeyBinary(null, new Object[] {1L})).isNull();
    }

    @Test
    void testBinarySearchConsistencyWithLinearSearch() {
        TableId tableId = TableId.parse("test_db.test_table");
        List<FinishedSnapshotSplitInfo> splits = new ArrayList<>();
        splits.add(createSplit(tableId, "split-0", null, new Object[] {100L}));
        splits.add(createSplit(tableId, "split-1", new Object[] {100L}, new Object[] {200L}));
        splits.add(createSplit(tableId, "split-2", new Object[] {200L}, new Object[] {300L}));
        splits.add(createSplit(tableId, "split-3", new Object[] {300L}, null));

        sortFinishedSplitInfos(splits);

        for (long key = 0; key < 400; key += 25) {
            Object[] keyArray = new Object[] {key};
            FinishedSnapshotSplitInfo binaryResult = findSplitByKeyBinary(splits, keyArray);

            FinishedSnapshotSplitInfo linearResult = null;
            for (FinishedSnapshotSplitInfo split : splits) {
                if (splitKeyRangeContains(keyArray, split.getSplitStart(), split.getSplitEnd())) {
                    linearResult = split;
                    break;
                }
            }

            if (binaryResult == null) {
                Assertions.assertThat(linearResult).isNull();
            } else {
                Assertions.assertThat(linearResult).isNotNull();
                Assertions.assertThat(binaryResult.getSplitId())
                        .isEqualTo(linearResult.getSplitId());
            }
        }
    }

    private FinishedSnapshotSplitInfo createSplit(
            TableId tableId, String splitId, Object[] splitStart, Object[] splitEnd) {
        OffsetFactory offsetFactory = Mockito.mock(OffsetFactory.class);
        Offset highWatermark = Mockito.mock(Offset.class);
        return new FinishedSnapshotSplitInfo(
                tableId, splitId, splitStart, splitEnd, highWatermark, offsetFactory);
    }
}
