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
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for the idle-publication fixes in {@link PostgresStreamingChangeEventSource}.
 *
 * <p>On an idle publication the WAL-position search loop blocks forever, because it waits for a
 * decoded WAL message while the only mechanism that would produce one on a quiet database (the
 * heartbeat action query) runs from heartbeat dispatch. The fix backports the two upstream Debezium
 * changes:
 *
 * <ul>
 *   <li>DBZ-6635 (2.4): dispatch heartbeats from the main streaming loop even before any position
 *       has been completely processed, so the heartbeat action query can generate WAL on a fresh
 *       start;
 *   <li>the 2.7 guard only entering the WAL-position search when {@code searchingEnabled() &&
 *       offsetContext.hasCompletelyProcessedPosition()}, plus dispatching heartbeats while
 *       searching.
 * </ul>
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

    private static PostgresOffsetContext freshStartOffsetContext(
            PostgresOffsetContext.Loader loader) {
        // A fresh stream split start: the starting offset carries an LSN (the low watermark) but
        // nothing has been completely processed yet.
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        return loader.load(offsetValues);
    }

    @Test
    void shouldNotSearchWalPositionOnFreshStart() {
        final PostgresOffsetContext offsetContext = freshStartOffsetContext(offsetLoader);

        // searchingEnabled() alone is true, so the pre-fix condition would enter the search loop
        // and stall forever on an idle publication...
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

    @Test
    void shouldStreamAndDispatchHeartbeatsOnIdlePublicationFreshStart() throws Exception {
        // Fresh start over an idle publication: the WAL-position search must be skipped and the
        // main streaming loop must keep dispatching heartbeats (which run the heartbeat action
        // query) even though nothing has a completely processed position. Without the DBZ-6635
        // backport the heartbeat dispatch stays blocked behind hasCompletelyProcessedPosition(),
        // no WAL is ever produced and the job stalls in the main loop instead of the search.
        final PostgresOffsetContext offsetContext = freshStartOffsetContext(offsetLoader);
        assertThat(offsetContext.hasCompletelyProcessedPosition()).isFalse();

        final PostgresTaskContext taskContext = mock(PostgresTaskContext.class);
        when(taskContext.getConfig()).thenReturn(connectorConfig);
        @SuppressWarnings("unchecked")
        final PostgresEventDispatcher<TableId> dispatcher = mock(PostgresEventDispatcher.class);
        final Snapshotter snapshotter = mock(Snapshotter.class);
        when(snapshotter.shouldStream()).thenReturn(true);
        final PostgresConnection connection = mock(PostgresConnection.class);
        final ErrorHandler errorHandler = mock(ErrorHandler.class);
        final PostgresReplicationConnection replicationConnection =
                mock(PostgresReplicationConnection.class);
        final ReplicationStream stream = mock(ReplicationStream.class);
        // Simulate an idle publication: no message is ever received.
        when(stream.readPending(any())).thenReturn(false);
        when(stream.startLsn()).thenReturn(Lsn.valueOf(12345L));
        when(replicationConnection.startStreaming(any(Lsn.class), any(WalPositionLocator.class)))
                .thenReturn(stream);

        final PostgresStreamingChangeEventSource source =
                new PostgresStreamingChangeEventSource(
                        connectorConfig,
                        snapshotter,
                        connection,
                        dispatcher,
                        errorHandler,
                        Clock.system(),
                        mock(PostgresSchema.class),
                        taskContext,
                        replicationConnection);

        // Run exactly two polls of the streaming loop, then stop the source.
        final AtomicInteger polls = new AtomicInteger();
        final ChangeEventSource.ChangeEventSourceContext context =
                () -> polls.getAndIncrement() < 2;

        source.execute(context, new PostgresPartition("test_server"), offsetContext);

        // execute() swallows throwables into the error handler.
        verify(errorHandler, never()).setProducerThrowable(any());
        // One heartbeat dispatch per poll, although no message was ever received.
        verify(dispatcher, times(2)).dispatchHeartbeatEvent(any(), eq(offsetContext));
        // The WAL-position search was skipped: streaming kept the initial connection.
        verify(replicationConnection, never()).reconnect();
    }
}
