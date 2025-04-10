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

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import oracle.sql.ROWID;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** IT tests to cover tables chunk splitter process. */
class OracleChunkSplitterTest extends OracleSourceTestBase {

    @Test
    void testIsChunkEndGeMax_Rowid_Case() throws SQLException {
        String a = "AAAzIdACKAAABWCAAA";
        String b = "AAAzIdAC/AACWIPAAB";
        rowidGeMaxCheck(a, b, true);
    }

    @Test
    void testIsChunkEndLeMax_Rowid_Case() throws SQLException {
        String a = "AAAzIdACKAAABWCAAA";
        String b = "AAAzIdAC/AACWIPAAB";
        rowidLeMaxCheck(a, b, true);
    }

    private void rowidGeMaxCheck(String chunkEndStr, String maxStr, boolean expected)
            throws SQLException {
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
        assertTrue(splitter.isChunkEndGeMax(jdbc, chunkEnd, max, null) == expected);
    }

    private void rowidLeMaxCheck(String chunkEndStr, String maxStr, boolean expected)
            throws SQLException {
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
        assertTrue(splitter.isChunkEndLeMax(jdbc, chunkEnd, max, null) == expected);
    }
}
