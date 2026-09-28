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

/**
 * Unit test for the MariaDB-flavored {@link BinlogOffset#compareTo(BinlogOffset)}. The shadowed
 * {@code BinlogOffset} compares GTID sets in MariaDB's {@code domain-server-sequence} format,
 * keying on {@code domain} and the highest {@code sequence} while ignoring the {@code server id}
 * (so a master and its replica compare equal for the same replicated history).
 */
class BinlogOffsetTest {

    @Test
    void testEqualIgnoresServerId() {
        // Same domain and sequence, different server id -> equal.
        BinlogOffset offset1 = BinlogOffset.builder().setGtidSet("0-1-100").build();
        BinlogOffset offset2 = BinlogOffset.builder().setGtidSet("0-2-100").build();
        assertCompareTo(offset1, offset2, 0);
        assertCompareTo(offset2, offset1, 0);
    }

    @Test
    void testEqualIsOrderIndependentAcrossDomains() {
        BinlogOffset offset1 = BinlogOffset.builder().setGtidSet("0-1-100,1-1-50").build();
        // Reordered domains and a different server id in each tuple -> still equal.
        BinlogOffset offset2 = BinlogOffset.builder().setGtidSet("1-2-50,0-3-100").build();
        assertCompareTo(offset1, offset2, 0);
        assertCompareTo(offset2, offset1, 0);
    }

    @Test
    void testGtidSetTakesPrecedenceOverBinlogPosition() {
        // When GTID sets are equal, the binlog file/position is ignored for the comparison.
        BinlogOffset offset1 =
                BinlogOffset.builder()
                        .setGtidSet("0-1-100")
                        .setBinlogFilePosition("mysql-bin.001", 123)
                        .build();
        BinlogOffset offset2 =
                BinlogOffset.builder()
                        .setGtidSet("0-2-100")
                        .setBinlogFilePosition("mysql-bin.001", 456)
                        .build();
        assertCompareTo(offset1, offset2, 0);
    }

    @Test
    void testContainmentKeysOnDomainAndSequence() {
        // "0-1-100" is contained within "0-1-200" (same domain, lower highest sequence).
        BinlogOffset sub = BinlogOffset.builder().setGtidSet("0-1-100").build();
        BinlogOffset sup = BinlogOffset.builder().setGtidSet("0-1-200").build();
        assertCompareTo(sub, sup, -1);
        assertCompareTo(sup, sub, 1);
    }

    @Test
    void testContainmentIgnoresServerIdOnFailover() {
        // The replica advanced past the checkpoint under a different server id; the checkpoint's
        // GTID must still be recognized as contained within the replica's larger set.
        BinlogOffset restored = BinlogOffset.builder().setGtidSet("0-1-100").build();
        BinlogOffset serverSet = BinlogOffset.builder().setGtidSet("0-2-150").build();
        assertCompareTo(restored, serverSet, -1);
        assertCompareTo(serverSet, restored, 1);
    }

    @Test
    void testDisjointDomainsAreNotContainedEitherWay() {
        BinlogOffset offset1 = BinlogOffset.builder().setGtidSet("0-1-100").build();
        BinlogOffset offset2 = BinlogOffset.builder().setGtidSet("1-1-100").build();
        // Neither contains the other, so the result is always 1 (mirrors upstream MySQL behavior).
        assertCompareTo(offset1, offset2, 1);
        assertCompareTo(offset2, offset1, 1);
    }

    @Test
    void testEqualGtidBreaksTieOnSkipEventsThenRows() {
        BinlogOffset offset1 =
                BinlogOffset.builder().setGtidSet("0-1-100").setSkipEvents(5).build();
        BinlogOffset offset2 =
                BinlogOffset.builder().setGtidSet("0-2-100").setSkipEvents(10).build();
        assertCompareTo(offset1, offset2, -1);
        assertCompareTo(offset2, offset1, 1);

        BinlogOffset offset3 =
                BinlogOffset.builder()
                        .setGtidSet("0-1-100")
                        .setSkipEvents(5)
                        .setSkipRows(10)
                        .build();
        BinlogOffset offset4 =
                BinlogOffset.builder()
                        .setGtidSet("0-2-100")
                        .setSkipEvents(5)
                        .setSkipRows(20)
                        .build();
        assertCompareTo(offset3, offset4, -1);
        assertCompareTo(offset4, offset3, 1);
    }

    @Test
    void testOffsetWithGtidIsAfterOffsetWithout() {
        BinlogOffset withGtid =
                BinlogOffset.builder()
                        .setGtidSet("0-1-100")
                        .setBinlogFilePosition("mysql-bin.001", 123)
                        .build();
        BinlogOffset withoutGtid =
                BinlogOffset.builder().setBinlogFilePosition("mysql-bin.001", 456).build();
        assertCompareTo(withGtid, withoutGtid, 1);
        assertCompareTo(withoutGtid, withGtid, -1);
    }

    private void assertCompareTo(BinlogOffset offset1, BinlogOffset offset2, int expected) {
        int actual = offset1.compareTo(offset2);
        // compareTo does not guarantee returning -1, 0, or 1. Just check the sign.
        Assertions.assertThat(Integer.signum(actual)).isEqualTo(expected);
    }
}
