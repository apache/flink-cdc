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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link
 * org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner}.
 */
class MySqlBinlogSplitAssignerTest {

    @Test
    void testStartFromEarliest() {
        checkAssignedBinlogOffset(StartupOptions.earliest(), BinlogOffset.ofEarliest());
    }

    @Test
    void testStartFromLatestOffset() {
        checkAssignedBinlogOffset(StartupOptions.latest(), BinlogOffset.ofLatest());
    }

    @Test
    void testStartFromTimestamp() {
        checkAssignedBinlogOffset(
                StartupOptions.timestamp(15213000L), BinlogOffset.ofTimestampSec(15213L));
    }

    @Test
    void testStartFromBinlogFile() {
        checkAssignedBinlogOffset(
                StartupOptions.specificOffset("foo-file", 15213),
                BinlogOffset.ofBinlogFilePosition("foo-file", 15213L));
    }

    @Test
    void testStartFromGtidSet() {
        checkAssignedBinlogOffset(
                StartupOptions.specificOffset("foo-gtid"), BinlogOffset.ofGtidSet("foo-gtid"));
    }

    private void checkAssignedBinlogOffset(
            StartupOptions startupOptions, BinlogOffset expectedOffset) {
        // Set starting from the given option
        MySqlBinlogSplitAssigner assigner = new MySqlBinlogSplitAssigner(getConfig(startupOptions));
        // Get splits from assigner
        Optional<MySqlSplit> optionalSplit = assigner.getNext();
        assertThat(optionalSplit).isPresent();
        MySqlBinlogSplit split = optionalSplit.get().asBinlogSplit();
        // Check binlog offset
        assertThat(split.getStartingOffset()).isEqualTo(expectedOffset);
        assertThat(split.getEndingOffset()).isEqualTo(BinlogOffset.ofNonStopping());
        // There should be only one split to assign
        assertThat(assigner.getNext()).isNotPresent();
        assigner.close();
    }

    private MySqlSourceConfig getConfig(StartupOptions startupOptions) {
        return new MySqlSourceConfigFactory()
                .startupOptions(startupOptions)
                .databaseList("foo-db")
                .tableList("foo-table")
                .hostname("foo-host")
                .port(15213)
                .username("jane-doe")
                .password("password")
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }
}
