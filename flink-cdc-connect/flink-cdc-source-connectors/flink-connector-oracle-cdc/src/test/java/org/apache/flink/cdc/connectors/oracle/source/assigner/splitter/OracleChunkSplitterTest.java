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

package org.apache.flink.cdc.connectors.oracle.source.assigner.splitter;

import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import oracle.sql.ROWID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.sql.SQLException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests to cover tables chunk splitter process. */
class OracleChunkSplitterTest extends OracleSourceTestBase {
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeEach
    public void before() throws Exception {

        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
        TestValuesTableFactory.clearAllData();

        env.setParallelism(1);
    }

    @Test
    void testIsChunkEndGeMax_Rowid_Case() throws SQLException {
        String a = "AAAzIdACKAAABWCAAA";
        String b = "AAAzIdAC/AACWIPAAB";
        rowidGeMaxCheck(a, b);
    }

    @Test
    void testIsChunkEndLeMax_Rowid_Case() throws SQLException {
        String a = "AAAzIdACKAAABWCAAA";
        String b = "AAAzIdAC/AACWIPAAB";
        rowidLeMaxCheck(a, b);
    }

    private void rowidGeMaxCheck(String chunkEndStr, String maxStr) throws SQLException {
        JdbcConfiguration jdbcConfig =
                JdbcConfiguration.create()
                        .with(JdbcConfiguration.HOSTNAME, ORACLE_CONTAINER.getHost())
                        .with(JdbcConfiguration.PORT, ORACLE_CONTAINER.getOraclePort())
                        .with(JdbcConfiguration.USER, ORACLE_CONTAINER.getUsername())
                        .with(JdbcConfiguration.PASSWORD, ORACLE_CONTAINER.getPassword())
                        .with(JdbcConfiguration.DATABASE, ORACLE_CONTAINER.getDatabaseName())
                        .build();
        JdbcConnection jdbc = OracleConnectionUtils.createOracleConnection(jdbcConfig);
        ROWID chunkEnd = new ROWID(chunkEndStr);
        ROWID max = new ROWID(maxStr);
        ChunkSplitterState chunkSplitterState = new ChunkSplitterState(null, null, null);
        OracleChunkSplitter splitter = new OracleChunkSplitter(null, null, chunkSplitterState);
        assertThat(splitter.isChunkEndGeMax(jdbc, chunkEnd, max, null)).isFalse();
    }

    private void rowidLeMaxCheck(String chunkEndStr, String maxStr) throws SQLException {
        JdbcConfiguration jdbcConfig =
                JdbcConfiguration.create()
                        .with(JdbcConfiguration.HOSTNAME, ORACLE_CONTAINER.getHost())
                        .with(JdbcConfiguration.PORT, ORACLE_CONTAINER.getOraclePort())
                        .with(JdbcConfiguration.USER, ORACLE_CONTAINER.getUsername())
                        .with(JdbcConfiguration.PASSWORD, ORACLE_CONTAINER.getPassword())
                        .with(JdbcConfiguration.DATABASE, ORACLE_CONTAINER.getDatabaseName())
                        .build();
        JdbcConnection jdbc = OracleConnectionUtils.createOracleConnection(jdbcConfig);
        ROWID chunkEnd = new ROWID(chunkEndStr);
        ROWID max = new ROWID(maxStr);
        ChunkSplitterState chunkSplitterState = new ChunkSplitterState(null, null, null);
        OracleChunkSplitter splitter = new OracleChunkSplitter(null, null, chunkSplitterState);
        assertThat(splitter.isChunkEndLeMax(jdbc, chunkEnd, max, null)).isTrue();
    }
}
