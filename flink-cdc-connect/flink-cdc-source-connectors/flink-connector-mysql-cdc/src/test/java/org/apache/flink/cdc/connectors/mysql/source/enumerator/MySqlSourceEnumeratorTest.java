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

package org.apache.flink.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaAssembledEvent;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.apache.flink.cdc.connectors.mysql.testutils.MetricsUtils.getMySqlSplitEnumeratorContext;

/** Tests for {@link MySqlSourceEnumerator}. */
class MySqlSourceEnumeratorTest {

    @Test
    void testBinlogSplitMetaAssembledEventIsNoOpForBinlogOnlyAssigner() {
        // For binlog-only startup modes the assigner is a MySqlBinlogSplitAssigner, not the hybrid
        // one. The reader still sends BinlogSplitMetaAssembledEvent, and the enumerator must treat
        // it as a safe no-op rather than failing.
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.latest())
                        .databaseList("db")
                        .tableList("db.t")
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("")
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .createConfig(0);
        MySqlSourceEnumerator enumerator =
                new MySqlSourceEnumerator(
                        getMySqlSplitEnumeratorContext(),
                        config,
                        new MySqlBinlogSplitAssigner(config),
                        Boundedness.CONTINUOUS_UNBOUNDED);

        Assertions.assertThatCode(
                        () ->
                                enumerator.handleSourceEvent(
                                        0, new BinlogSplitMetaAssembledEvent(0L)))
                .doesNotThrowAnyException();
    }
}
