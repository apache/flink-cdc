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

package org.apache.flink.cdc.connectors.mysql.source.offset;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for {@link BinlogOffset}. */
public class BinlogOffsetTest {
    public static final String PART_OF_GTID_SET_1 = "abcd:1-4";
    public static final String PART_OF_GTID_SET_2 = "efgh:1-10";
    public static final String FULL_GTID_SET =
            String.join(",", PART_OF_GTID_SET_1, PART_OF_GTID_SET_2);

    @Test
    public void testCompareToWithGtidSet() {
        // Test same GTID sets in different orders
        BinlogOffset offset1 = BinlogOffset.builder().setGtidSet(FULL_GTID_SET).build();
        BinlogOffset offset2 =
                BinlogOffset.builder()
                        .setGtidSet(String.join(",", PART_OF_GTID_SET_2, PART_OF_GTID_SET_1))
                        .build();
        assetCompareTo(offset1, offset2, 0);

        // The test uses GTID instead of position for comparison.
        offset1 =
                BinlogOffset.builder()
                        .setGtidSet(FULL_GTID_SET)
                        .setBinlogFilePosition("binlog.001", 123)
                        .build();
        offset2 =
                BinlogOffset.builder()
                        .setGtidSet(String.join(",", PART_OF_GTID_SET_2, PART_OF_GTID_SET_1))
                        .setBinlogFilePosition("binlog.001", 456)
                        .build();
        assetCompareTo(offset1, offset2, 0);

        // Test different GTID sets where one contains another
        BinlogOffset offset3 = BinlogOffset.builder().setGtidSet(PART_OF_GTID_SET_1).build();
        BinlogOffset offset4 =
                BinlogOffset.builder()
                        .setGtidSet("abcd:1-5") // Contains offset3's GTID set
                        .build();

        // offset3 should be before offset4
        assetCompareTo(offset3, offset4, -1);
        assetCompareTo(offset4, offset3, 1);

        // The test uses GTID instead of position for comparison.
        offset3 =
                BinlogOffset.builder()
                        .setGtidSet(PART_OF_GTID_SET_1)
                        .setBinlogFilePosition("binlog.001", 1000)
                        .build();
        offset4 =
                BinlogOffset.builder()
                        .setGtidSet("abcd:1-5") // Contains offset3's GTID set
                        .setBinlogFilePosition("binlog.001", 23)
                        .build();
        assetCompareTo(offset3, offset4, -1);
        assetCompareTo(offset4, offset3, 1);

        // Test completely different GTID sets
        BinlogOffset offset5 = BinlogOffset.builder().setGtidSet(PART_OF_GTID_SET_1).build();
        BinlogOffset offset6 = BinlogOffset.builder().setGtidSet(PART_OF_GTID_SET_2).build();

        // offsets don't contain each other, result is always 1
        assetCompareTo(offset5, offset6, 1);
        assetCompareTo(offset6, offset5, 1);
    }

    @Test
    public void testCompareToWithGtidSetAndSkipEventsAndSkipRows() {
        // Test same GTID but different skip events
        BinlogOffset offset1 =
                BinlogOffset.builder().setGtidSet(FULL_GTID_SET).setSkipEvents(5).build();
        BinlogOffset offset2 =
                BinlogOffset.builder().setGtidSet(FULL_GTID_SET).setSkipEvents(10).build();

        assetCompareTo(offset1, offset2, -1);
        assetCompareTo(offset2, offset1, 1);

        // Test same GTID and skip events but different skip rows
        BinlogOffset offset3 =
                BinlogOffset.builder()
                        .setGtidSet(FULL_GTID_SET)
                        .setSkipEvents(5)
                        .setSkipRows(10)
                        .build();
        BinlogOffset offset4 =
                BinlogOffset.builder()
                        .setGtidSet(FULL_GTID_SET)
                        .setSkipEvents(5)
                        .setSkipRows(20)
                        .build();

        assetCompareTo(offset3, offset4, -1);
        assetCompareTo(offset4, offset3, 1);
    }

    @Test
    public void testCompareToWithGtidSetExistence() {
        // Test one offset has GTID set and another doesn't
        BinlogOffset offsetWithGtid =
                BinlogOffset.builder()
                        .setGtidSet(PART_OF_GTID_SET_1)
                        .setBinlogFilePosition("binlog.001", 123)
                        .build();
        BinlogOffset offsetWithoutGtid =
                BinlogOffset.builder().setBinlogFilePosition("binlog.001", 456).build();

        // When one has GTID and another doesn't, the one without GTID is considered older
        assetCompareTo(offsetWithGtid, offsetWithoutGtid, 1);
        assetCompareTo(offsetWithoutGtid, offsetWithGtid, -1);

        // Test the reverse scenario
        BinlogOffset offsetWithGtid2 =
                BinlogOffset.builder().setGtidSet(PART_OF_GTID_SET_2).build();
        BinlogOffset offsetWithoutGtid2 =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.002", 789)
                        .setSkipEvents(5)
                        .build();

        assetCompareTo(offsetWithGtid2, offsetWithoutGtid2, 1);
        assetCompareTo(offsetWithoutGtid2, offsetWithGtid2, -1);
    }

    @Test
    public void testCompareToWithFilePosition() {
        // Test same file position - should be equal
        BinlogOffset offset1 =
                BinlogOffset.builder().setBinlogFilePosition("binlog.001", 123).build();
        BinlogOffset offset2 =
                BinlogOffset.builder().setBinlogFilePosition("binlog.001", 123).build();
        assetCompareTo(offset1, offset2, 0);

        // Test different file names
        BinlogOffset offset3 =
                BinlogOffset.builder().setBinlogFilePosition("binlog.001", 123).build();
        BinlogOffset offset4 =
                BinlogOffset.builder().setBinlogFilePosition("binlog.002", 123).build();
        assetCompareTo(offset3, offset4, -1);
        assetCompareTo(offset4, offset3, 1);

        // Test different positions in same file
        BinlogOffset offset5 =
                BinlogOffset.builder().setBinlogFilePosition("binlog.001", 100).build();
        BinlogOffset offset6 =
                BinlogOffset.builder().setBinlogFilePosition("binlog.001", 200).build();
        assetCompareTo(offset5, offset6, -1);
        assetCompareTo(offset6, offset5, 1);
    }

    @Test
    public void testCompareToWithFilePositionAndSkipEventsAndSkipRows() {
        // Test with skip events
        BinlogOffset offset1 =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.001", 123)
                        .setSkipEvents(5)
                        .build();
        BinlogOffset offset2 =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.001", 123)
                        .setSkipEvents(10)
                        .build();
        assetCompareTo(offset1, offset2, -1);
        assetCompareTo(offset2, offset1, 1);

        // Test with skip rows
        BinlogOffset offset3 =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.001", 123)
                        .setSkipEvents(5)
                        .setSkipRows(10)
                        .build();
        BinlogOffset offset4 =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.001", 123)
                        .setSkipEvents(5)
                        .setSkipRows(20)
                        .build();
        assetCompareTo(offset3, offset4, -1);
        assetCompareTo(offset4, offset3, 1);
    }

    @Test
    public void testCompareToTimestampWithDifferentServerId() {
        // Test different server IDs with different timestamps
        BinlogOffset offset1 =
                BinlogOffset.builder()
                        .setServerId(1L)
                        .setTimestampSec(1000L)
                        .setBinlogFilePosition("binlog.001", 123)
                        .build();
        BinlogOffset offset2 =
                BinlogOffset.builder()
                        .setServerId(2L)
                        .setTimestampSec(2000L)
                        .setBinlogFilePosition("binlog.001", 123)
                        .build();

        // Should compare based on timestamp since server IDs are different
        assetCompareTo(offset1, offset2, -1);
        assetCompareTo(offset2, offset1, 1);

        // Test same timestamps but different server IDs
        BinlogOffset offset3 =
                BinlogOffset.builder()
                        .setServerId(1L)
                        .setTimestampSec(1500L)
                        .setBinlogFilePosition("binlog.001", 432)
                        .build();
        BinlogOffset offset4 =
                BinlogOffset.builder()
                        .setServerId(2L)
                        .setTimestampSec(1500L)
                        .setBinlogFilePosition("binlog.001", 123)
                        .build();

        // Same timestamp, different server IDs - should compare based on timestamp (which are
        // equal)
        // But since server IDs are different and timestamps are same, it will fall through to file
        // position comparison
        // Since file positions are same, it will compare skip events (default 0)
        assetCompareTo(offset3, offset4, 0);
    }

    private void assetCompareTo(BinlogOffset offset1, BinlogOffset offset2, int expected) {
        int actual = offset1.compareTo(offset2);
        Assertions.assertThat(expected).isEqualTo(actual);
    }
}
