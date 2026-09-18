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

package io.debezium.connector.postgresql;

import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for the WAL-position-search guard in {@link PostgresStreamingChangeEventSource}.
 *
 * <p>On an idle publication the search loop blocks forever, because it waits for a decoded WAL
 * message while the only mechanism that would produce one on a quiet database (the heartbeat action
 * query) runs from the main streaming loop that the search precedes. The fix — mirroring Debezium
 * 2.7 — only enters the search when the stored offset has actually processed a position, i.e. when
 * {@code searchingEnabled() && offsetContext.hasCompletelyProcessedPosition()}. This test pins that
 * decision boundary for both a fresh start and a resumed offset.
 */
class PostgresStreamingChangeEventSourceTest {

    private PostgresConnectorConfig connectorConfig;
    private PostgresOffsetContext.Loader offsetLoader;

    @BeforeEach
    public void beforeEach() {
        this.connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new PostgresOffsetContext.Loader(this.connectorConfig);
    }

    /**
     * Builds the {@link WalPositionLocator} the same way {@code execute} does for a stored offset.
     */
    private static WalPositionLocator walPositionFor(PostgresOffsetContext offsetContext) {
        Lsn lsn =
                offsetContext.lastCompletelyProcessedLsn() != null
                        ? offsetContext.lastCompletelyProcessedLsn()
                        : offsetContext.lsn();
        return new WalPositionLocator(offsetContext.lastCommitLsn(), lsn);
    }

    @Test
    void shouldNotSearchWalPositionOnFreshStart() {
        // A fresh stream split start: the starting offset carries an LSN (the low watermark) but
        // nothing has been completely processed yet.
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);

        final PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);

        // searchingEnabled() alone is true, so the pre-fix condition would enter the search loop
        // and
        // stall forever on an idle publication...
        assertThat(walPositionFor(offsetContext).searchingEnabled()).isTrue();
        // ...but the added guard is false on a fresh start, so the search is correctly skipped.
        assertThat(offsetContext.hasCompletelyProcessedPosition())
                .as(
                        "WAL search must be skipped on a fresh start so an idle publication cannot stall it")
                .isFalse();
    }

    @Test
    void shouldSearchWalPositionWhenResumingFromProcessedOffset() {
        // A resumed offset (e.g. after a checkpoint): a position has already been processed, so the
        // search is still required to locate the exact resume point among already-seen LSNs.
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 12345L);

        final PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);

        // Both the pre-fix condition and the added guard are true, so the search still runs.
        assertThat(walPositionFor(offsetContext).searchingEnabled()).isTrue();
        assertThat(offsetContext.hasCompletelyProcessedPosition())
                .as("WAL search must still run when resuming from an already-processed position")
                .isTrue();
    }
}
