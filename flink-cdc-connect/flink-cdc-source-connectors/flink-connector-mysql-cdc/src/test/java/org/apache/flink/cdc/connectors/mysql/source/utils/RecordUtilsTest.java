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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.findSplitByKeyBinary;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.splitKeyRangeContains;

/** Tests for {@link org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils}. */
class RecordUtilsTest {

    @Test
    void testSplitKeyRangeContains() {
        // table with only one split
        assertKeyRangeContains(new Object[] {100L}, null, null);
        // the last split
        assertKeyRangeContains(new Object[] {101L}, new Object[] {100L}, null);

        // the first split
        assertKeyRangeContains(new Object[] {101L}, null, new Object[] {1024L});

        // general splits
        assertKeyRangeContains(new Object[] {100L}, new Object[] {1L}, new Object[] {1024L});
        Assertions.assertThat(
                        splitKeyRangeContains(
                                new Object[] {0L}, new Object[] {1L}, new Object[] {1024L}))
                .isFalse();

        // split key from binlog may have different type
        assertKeyRangeContains(
                new Object[] {BigInteger.valueOf(100L)}, new Object[] {1L}, new Object[] {1024L});
        Assertions.assertThat(
                        splitKeyRangeContains(
                                new Object[] {BigInteger.valueOf(0L)},
                                new Object[] {1L},
                                new Object[] {1024L}))
                .isFalse();
    }

    @Test
    void testDifferentKeyTypes() {
        // first split
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Byte.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Short.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Integer.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Long.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {BigInteger.valueOf(6)});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {BigDecimal.valueOf(6)});

        // other splits
        assertKeyRangeContains(
                new Object[] {Byte.valueOf("6")},
                new Object[] {Byte.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Short.valueOf("60")},
                new Object[] {Short.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Integer.valueOf("600")},
                new Object[] {Integer.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Long.valueOf("6000")},
                new Object[] {Long.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {BigInteger.valueOf(60000)},
                new Object[] {BigInteger.valueOf(6)},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {BigDecimal.valueOf(60000)},
                new Object[] {BigDecimal.valueOf(6)},
                new Object[] {BigDecimal.valueOf(100000L)});

        // last split
        assertKeyRangeContains(new Object[] {7}, new Object[] {Byte.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Short.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Integer.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Long.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {BigInteger.valueOf(6)}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {BigDecimal.valueOf(6)}, null);
    }

    @Test
    void testSortFinishedSplitInfos() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> emptyList = new ArrayList<>();
        RecordUtils.sortFinishedSplitInfos(emptyList);
        Assertions.assertThat(emptyList).isEmpty();

        List<FinishedSnapshotSplitInfo> singleList = new ArrayList<>();
        singleList.add(createSplit(tableId, "split-1", new Object[] {100L}, new Object[] {200L}));
        RecordUtils.sortFinishedSplitInfos(singleList);
        Assertions.assertThat(singleList).hasSize(1);

        List<FinishedSnapshotSplitInfo> splits = new ArrayList<>();
        splits.add(createSplit(tableId, "split-3", new Object[] {200L}, new Object[] {300L}));
        splits.add(createSplit(tableId, "split-1", null, new Object[] {100L}));
        splits.add(createSplit(tableId, "split-4", new Object[] {300L}, null));
        splits.add(createSplit(tableId, "split-2", new Object[] {100L}, new Object[] {200L}));

        RecordUtils.sortFinishedSplitInfos(splits);

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

        FinishedSnapshotSplitInfo result = findSplitByKeyBinary(sortedSplits, new Object[] {-1L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {100L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-1");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {150L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-1");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {200L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-2");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {250L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-2");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {300L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-3");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {1000L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-3");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {99L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");
    }

    @Test
    void testFindSplitByKeyBinaryWithOnlyOneSplit() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> sortedSplits = new ArrayList<>();
        sortedSplits.add(createSplit(tableId, "split-0", null, null));

        FinishedSnapshotSplitInfo result = findSplitByKeyBinary(sortedSplits, new Object[] {100L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {Long.MAX_VALUE});
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

        FinishedSnapshotSplitInfo result = findSplitByKeyBinary(sortedSplits, new Object[] {5L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-0");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {505L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-50");

        result = findSplitByKeyBinary(sortedSplits, new Object[] {9995L});
        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.getSplitId()).isEqualTo("split-999");
    }

    @Test
    void testFindSplitByKeyBinaryEdgeCases() {
        TableId tableId = new TableId("test_db", null, "test_table");

        List<FinishedSnapshotSplitInfo> emptyList = new ArrayList<>();
        FinishedSnapshotSplitInfo result = findSplitByKeyBinary(emptyList, new Object[] {100L});
        Assertions.assertThat(result).isNull();

        result = findSplitByKeyBinary(null, new Object[] {100L});
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

            FinishedSnapshotSplitInfo binaryResult = findSplitByKeyBinary(sortedSplits, keyArray);

            FinishedSnapshotSplitInfo linearResult = null;
            for (FinishedSnapshotSplitInfo split : sortedSplits) {
                if (splitKeyRangeContains(keyArray, split.getSplitStart(), split.getSplitEnd())) {
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

    private void assertKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        Assertions.assertThat(splitKeyRangeContains(key, splitKeyStart, splitKeyEnd)).isTrue();
    }
}
