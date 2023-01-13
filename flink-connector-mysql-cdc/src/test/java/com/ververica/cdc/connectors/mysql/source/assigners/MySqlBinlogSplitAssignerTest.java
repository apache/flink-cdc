/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.assigners;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Unit test for {@link MySqlBinlogSplitAssigner}. */
public class MySqlBinlogSplitAssignerTest {

    @Test
    public void testStartFromEarliest() {
        checkAssignedBinlogOffset(StartupOptions.earliest(), BinlogOffset.ofEarliest());
    }

    @Test
    public void testStartFromLatestOffset() {
        checkAssignedBinlogOffset(StartupOptions.latest(), BinlogOffset.ofLatest());
    }

    @Test
    public void testStartFromTimestamp() {
        checkAssignedBinlogOffset(
                StartupOptions.timestamp(15213000L), BinlogOffset.ofTimestampSec(15213L));
    }

    @Test
    public void testStartFromBinlogFile() {
        checkAssignedBinlogOffset(
                StartupOptions.specificOffset("foo-file", 15213),
                BinlogOffset.ofBinlogFilePosition("foo-file", 15213L));
    }

    @Test
    public void testStartFromGtidSet() {
        checkAssignedBinlogOffset(
                StartupOptions.specificOffset("foo-gtid"), BinlogOffset.ofGtidSet("foo-gtid"));
    }

    private void checkAssignedBinlogOffset(
            StartupOptions startupOptions, BinlogOffset expectedOffset) {
        // Set starting from the given option
        MySqlBinlogSplitAssigner assigner = new MySqlBinlogSplitAssigner(getConfig(startupOptions));
        // Get splits from assigner
        Optional<MySqlSplit> optionalSplit = assigner.getNext();
        assertTrue(optionalSplit.isPresent());
        MySqlBinlogSplit split = optionalSplit.get().asBinlogSplit();
        // Check binlog offset
        assertEquals(expectedOffset, split.getStartingOffset());
        assertEquals(BinlogOffset.ofNonStopping(), split.getEndingOffset());
        // There should be only one split to assign
        assertFalse(assigner.getNext().isPresent());
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
