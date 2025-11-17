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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link SplitKeyUtils}. */
class SplitKeyUtilsTest {

    @Test
    void testSortFinishedSplitInfos() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> emptyList = new ArrayList<>();
        SplitKeyUtils.sortFinishedSplitInfos(emptyList);
        Assertions.assertThat(emptyList).isEmpty();

        List<FinishedSnapshotSplitInfo> singleList = new ArrayList<>();
        singleList.add(createSplit(tableId, "split-1", new Object[] {100L}, new Object[] {200L}));
        SplitKeyUtils.sortFinishedSplitInfos(singleList);
        Assertions.assertThat(singleList).hasSize(1);

        List<FinishedSnapshotSplitInfo> splits = new ArrayList<>();
        splits.add(createSplit(tableId, "split-3", new Object[] {200L}, new Object[] {300L}));
        splits.add(createSplit(tableId, "split-1", null, new Object[] {100L}));
        splits.add(createSplit(tableId, "split-4", new Object[] {300L}, null));
        splits.add(createSplit(tableId, "split-2", new Object[] {100L}, new Object[] {200L}));

        SplitKeyUtils.sortFinishedSplitInfos(splits);

        Assertions.assertThat(splits.get(0).getSplitStart()).isNull();
        Assertions.assertThat(splits.get(1).getSplitStart()).isEqualTo(new Object[] {100L});
        Assertions.assertThat(splits.get(2).getSplitStart()).isEqualTo(new Object[] {200L});
        Assertions.assertThat(splits.get(3).getSplitStart()).isEqualTo(new Object[] {300L});
    }

    @Test
    void testFindSplitByKeyBinary() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> sortedSplits = new ArrayList<>();
        sortedSplits.add(createSplit(tableId, "split-0", null, new Object[] {100L}));
        sortedSplits.add(createSplit(tableId, "split-1", new Object[] {100L}, new Object[] {200L}));
        sortedSplits.add(createSplit(tableId, "split-2", new Object[] {200L}, new Object[] {300L}));
        sortedSplits.add(createSplit(tableId, "split-3", new Object[] {300L}, null));

        FinishedSnapshotSplitInfo result =
                SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {-1L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {100L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-1");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {150L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-1");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {200L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-2");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {250L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-2");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {300L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-3");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {1000L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-3");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {99L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");
    }

    @Test
    void testFindSplitByKeyBinaryWithOnlyOneSplit() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> sortedSplits = new ArrayList<>();
        sortedSplits.add(createSplit(tableId, "split-0", null, null));

        FinishedSnapshotSplitInfo result =
                SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {100L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {Long.MAX_VALUE});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");
    }

    @Test
    void testFindSplitByKeyBinaryWithLargeNumberOfSplits() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> sortedSplits = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            Object[] start = i == 0 ? null : new Object[] {(long) i * 10};
            Object[] end = i == 999 ? null : new Object[] {(long) (i + 1) * 10};
            sortedSplits.add(createSplit(tableId, "split-" + i, start, end));
        }

        FinishedSnapshotSplitInfo result =
                SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {5L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {505L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-50");

        result = SplitKeyUtils.findSplitByKeyBinary(sortedSplits, new Object[] {9995L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-999");
    }

    @Test
    void testFindSplitByKeyBinaryEdgeCases() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> emptyList = new ArrayList<>();
        FinishedSnapshotSplitInfo result =
                SplitKeyUtils.findSplitByKeyBinary(emptyList, new Object[] {100L});
        Assertions.assertThat(result).isNull();

        result = SplitKeyUtils.findSplitByKeyBinary(null, new Object[] {100L});
        Assertions.assertThat(result).isNull();
    }

    @Test
    void testBinarySearchConsistencyWithLinearSearch() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> sortedSplits = new ArrayList<>();
        sortedSplits.add(createSplit(tableId, "split-0", null, new Object[] {100L}));
        sortedSplits.add(createSplit(tableId, "split-1", new Object[] {100L}, new Object[] {200L}));
        sortedSplits.add(createSplit(tableId, "split-2", new Object[] {200L}, new Object[] {300L}));
        sortedSplits.add(createSplit(tableId, "split-3", new Object[] {300L}, new Object[] {400L}));
        sortedSplits.add(createSplit(tableId, "split-4", new Object[] {400L}, null));

        for (long key = 0; key < 500; key += 10) {
            Object[] keyArray = new Object[] {key};

            FinishedSnapshotSplitInfo binaryResult =
                    SplitKeyUtils.findSplitByKeyBinary(sortedSplits, keyArray);

            FinishedSnapshotSplitInfo linearResult = null;
            for (FinishedSnapshotSplitInfo split : sortedSplits) {
                if (SplitKeyUtils.splitKeyRangeContains(
                        keyArray, split.getSplitStart(), split.getSplitEnd())) {
                    linearResult = split;
                    break;
                }
            }

            if (binaryResult == null) {
                Assertions.assertThat(linearResult)
                        .as("Key %d should not be in any split", key)
                        .isNull();
            } else {
                Assertions.assertThat(linearResult)
                        .as("Key %d should be found by both methods", key)
                        .isNotNull();
                Assertions.assertThat(binaryResult.getSplitId())
                        .as("Both methods should find the same split for key %d", key)
                        .isEqualTo(linearResult.getSplitId());
            }
        }
    }

    private FinishedSnapshotSplitInfo createSplit(
            TableId tableId, String splitId, Object[] splitStart, Object[] splitEnd) {
        return new FinishedSnapshotSplitInfo(
                tableId, splitId, splitStart, splitEnd, BinlogOffset.ofEarliest());
    }
}
