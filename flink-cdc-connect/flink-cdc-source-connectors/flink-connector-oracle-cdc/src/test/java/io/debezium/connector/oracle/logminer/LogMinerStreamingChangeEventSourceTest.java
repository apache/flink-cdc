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

package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.Scn;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for extracted LogMiner resilience helpers. */
class LogMinerStreamingChangeEventSourceTest {

    @Test
    void testFinalizeEndScnReturnsNullForMissingCandidate() {
        assertThat(LogMinerStreamingChangeEventSource.finalizeEndScn(null, Scn.valueOf(100L)))
                .isEqualTo(Scn.NULL);
    }

    @Test
    void testFinalizeEndScnReturnsNullWhenCandidateDoesNotAdvance() {
        assertThat(
                        LogMinerStreamingChangeEventSource.finalizeEndScn(
                                Scn.valueOf(100L), Scn.valueOf(100L)))
                .isEqualTo(Scn.NULL);
    }

    @Test
    void testFinalizeEndScnKeepsAdvancingCandidate() {
        assertThat(
                        LogMinerStreamingChangeEventSource.finalizeEndScn(
                                Scn.valueOf(101L), Scn.valueOf(100L)))
                .isEqualTo(Scn.valueOf(101L));
    }

    @Test
    void testClampEndScnToRedoThreadWindowKeepsCandidateWithoutRedoThreadScn() {
        assertThat(
                        LogMinerStreamingChangeEventSource.clampEndScnToRedoThreadWindow(
                                Scn.valueOf(120L), Scn.valueOf(100L), null))
                .isEqualTo(Scn.valueOf(120L));
    }

    @Test
    void testClampEndScnToRedoThreadWindowRestrictsUpperBound() {
        assertThat(
                        LogMinerStreamingChangeEventSource.clampEndScnToRedoThreadWindow(
                                Scn.valueOf(120L), Scn.valueOf(100L), Scn.valueOf(110L)))
                .isEqualTo(Scn.valueOf(109L));
    }

    @Test
    void testClampEndScnToRedoThreadWindowReturnsNullWhenBoundFallsBeforeStart() {
        assertThat(
                        LogMinerStreamingChangeEventSource.clampEndScnToRedoThreadWindow(
                                Scn.valueOf(120L), Scn.valueOf(100L), Scn.valueOf(100L)))
                .isEqualTo(Scn.NULL);
    }

    @Test
    void testGetRedoThreadTableNameUsesLocalViewWithoutRac() {
        assertThat(LogMinerStreamingChangeEventSource.getRedoThreadTableName(null))
                .isEqualTo("V$THREAD");
        assertThat(
                        LogMinerStreamingChangeEventSource.getRedoThreadTableName(
                                Collections.emptySet()))
                .isEqualTo("V$THREAD");
    }

    @Test
    void testGetRedoThreadTableNameUsesGlobalViewWithRac() {
        Set<String> racNodes = Collections.singleton("node-1");
        assertThat(LogMinerStreamingChangeEventSource.getRedoThreadTableName(racNodes))
                .isEqualTo("GV$THREAD");
    }

    @Test
    void testIsOracleErrorMatchesCodeOrMessage() {
        SQLException codeOnly = new SQLException("unrelated", "state", 1292);
        SQLException messageOnly = new SQLException("ORA-01292 missing log files");
        SQLException mismatch = new SQLException("ORA-99999", "state", 99999);

        assertThat(LogMinerStreamingChangeEventSource.isOracleError(codeOnly, 1292, "ORA-01292"))
                .isTrue();
        assertThat(LogMinerStreamingChangeEventSource.isOracleError(messageOnly, 1292, "ORA-01292"))
                .isTrue();
        assertThat(LogMinerStreamingChangeEventSource.isOracleError(mismatch, 1292, "ORA-01292"))
                .isFalse();
    }
}
