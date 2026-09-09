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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BinlogLagCalculator}. */
class BinlogLagCalculatorTest {

    private final BinlogLagCalculator calculator = new BinlogLagCalculator();

    // --- GTID mode tests ---

    @Test
    void testGtidLagWhenFullyCaughtUp() {
        String gtidSet = "uuid1:1-100";
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofGtidSet(gtidSet), BinlogOffset.ofGtidSet(gtidSet));
        assertThat(result.getTransactionLag()).isEqualTo(0);
    }

    @Test
    void testGtidLagWithSingleUuid() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofGtidSet("uuid1:1-150"),
                        BinlogOffset.ofGtidSet("uuid1:1-200"));
        assertThat(result.getTransactionLag()).isEqualTo(50);
    }

    @Test
    void testGtidLagWithMultipleUuids() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofGtidSet("uuid1:1-80,uuid2:1-50"),
                        BinlogOffset.ofGtidSet("uuid1:1-100,uuid2:1-50"));
        assertThat(result.getTransactionLag()).isEqualTo(20);
    }

    @Test
    void testGtidLagWithNewUuidInMaster() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofGtidSet("uuid1:1-100"),
                        BinlogOffset.ofGtidSet("uuid1:1-100,uuid2:1-30"));
        assertThat(result.getTransactionLag()).isEqualTo(30);
    }

    @Test
    void testGtidLagWithDisjointIntervals() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofGtidSet("uuid1:1-50"),
                        BinlogOffset.ofGtidSet("uuid1:1-50:100-200"));
        assertThat(result.getTransactionLag()).isEqualTo(150);
    }

    @Test
    void testGtidLagWhenCurrentStartsFromMiddle() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofGtidSet(
                                "e5f89193-ec81-11ec-8f1c-525400c689b3:1774595494-1775564172"),
                        BinlogOffset.ofGtidSet(
                                "e5f89193-ec81-11ec-8f1c-525400c689b3:1-1793121110"));
        assertThat(result.getTransactionLag()).isEqualTo(17556938);
    }

    @Test
    void testSameGtidFallsBackToPositionComparison() {
        String sameGtid = "c7d9df75-8210-11eb-b9fb-02483243d90e:1-929560837";
        BinlogOffset current =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.000040", 5000)
                        .setGtidSet(sameGtid)
                        .build();
        BinlogOffset master =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.000040", 8000)
                        .setGtidSet(sameGtid)
                        .build();
        BinlogLagCalculator.LagResult result = calculator.calculateLag(current, master);
        assertThat(result.getTransactionLag()).isEqualTo(0);
        assertThat(result.getBytePositionLag()).isEqualTo(3000);
    }

    @Test
    void testSameGtidCurrentAheadOfMaster() {
        String sameGtid = "c7d9df75-8210-11eb-b9fb-02483243d90e:1-929560837";
        BinlogOffset current =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.000040", 9000)
                        .setGtidSet(sameGtid)
                        .build();
        BinlogOffset master =
                BinlogOffset.builder()
                        .setBinlogFilePosition("binlog.000040", 5000)
                        .setGtidSet(sameGtid)
                        .build();
        BinlogLagCalculator.LagResult result = calculator.calculateLag(current, master);
        assertThat(result.getTransactionLag()).isEqualTo(0);
        assertThat(result.getBytePositionLag()).isEqualTo(0);
    }

    // --- Non-GTID mode tests ---

    @Test
    void testFilePositionLagSameFile() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000003", 1000),
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000003", 5000));
        assertThat(result.getTransactionLag()).isEqualTo(-1);
        assertThat(result.getBytePositionLag()).isEqualTo(4000);
    }

    @Test
    void testFilePositionLagSameFileCaughtUp() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000003", 5000),
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000003", 5000));
        assertThat(result.getBytePositionLag()).isEqualTo(0);
    }

    @Test
    void testFilePositionLagSameFileNeverNegative() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000003", 5100),
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000003", 5000));
        assertThat(result.getBytePositionLag()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testFilePositionLagCrossFile() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000003", 1000),
                        BinlogOffset.ofBinlogFilePosition("mysql-bin.000005", 2000));
        assertThat(result.getBytePositionLag()).isEqualTo(2 * 1_000_000L + 2000 - 1000);
    }

    @Test
    void testReturnsNonNegativeWhenBothOffsetsAreEarliest() {
        BinlogLagCalculator.LagResult result =
                calculator.calculateLag(BinlogOffset.ofEarliest(), BinlogOffset.ofEarliest());
        assertThat(result.getBytePositionLag()).isGreaterThanOrEqualTo(-1);
    }

    // --- extractFileSequence tests ---

    @Test
    void testExtractFileSequence() {
        assertThat(BinlogLagCalculator.extractFileSequence("mysql-bin.000003")).isEqualTo(3);
        assertThat(BinlogLagCalculator.extractFileSequence("mysql-bin.000100")).isEqualTo(100);
    }
}
