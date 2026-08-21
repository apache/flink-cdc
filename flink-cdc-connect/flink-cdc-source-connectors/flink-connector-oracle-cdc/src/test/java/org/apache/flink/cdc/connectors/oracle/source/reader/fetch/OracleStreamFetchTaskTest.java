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

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for adapter split capability checks in {@link OracleStreamFetchTask}. */
class OracleStreamFetchTaskTest {

    @Test
    void testAllowUnboundedSplitForXstreamAdapter() {
        assertThatCode(
                        () ->
                                OracleStreamFetchTask.validateAdapterSupportsSplit(
                                        connectorConfig("xstream"),
                                        streamSplit(
                                                new RedoLogOffset(1L),
                                                RedoLogOffset.NO_STOPPING_OFFSET)))
                .doesNotThrowAnyException();
    }

    @Test
    void testAllowBoundedSplitForLogMinerAdapter() {
        assertThatCode(
                        () ->
                                OracleStreamFetchTask.validateAdapterSupportsSplit(
                                        connectorConfig("logminer"),
                                        streamSplit(new RedoLogOffset(1L), new RedoLogOffset(2L))))
                .doesNotThrowAnyException();
    }

    @Test
    void testRejectBoundedSplitForXstreamAdapter() {
        assertThatThrownBy(
                        () ->
                                OracleStreamFetchTask.validateAdapterSupportsSplit(
                                        connectorConfig("xstream"),
                                        streamSplit(new RedoLogOffset(1L), new RedoLogOffset(2L))))
                .hasMessageContaining("xstream")
                .hasMessageContaining("bounded stream split backfill");
    }

    @Test
    void testAllowBoundedSplitForDefaultAdapter() {
        assertThatCode(
                        () ->
                                OracleStreamFetchTask.validateAdapterSupportsSplit(
                                        connectorConfig(null),
                                        streamSplit(new RedoLogOffset(1L), new RedoLogOffset(2L))))
                .doesNotThrowAnyException();
    }

    @Test
    void testCloseStopsRunningChangeEventSourceContext() {
        OracleStreamFetchTask fetchTask =
                new OracleStreamFetchTask(
                        streamSplit(new RedoLogOffset(1L), RedoLogOffset.NO_STOPPING_OFFSET));
        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        fetchTask.registerRunningChangeEventSource(changeEventSourceContext, null);

        fetchTask.close();

        assertThat(changeEventSourceContext.isRunning()).isFalse();
        assertThat(fetchTask.isRunning()).isFalse();
    }

    @Test
    void testCommitCurrentOffsetSuppressesDuplicateAndRollbackLcrPositions() {
        OracleStreamFetchTask fetchTask =
                new OracleStreamFetchTask(
                        streamSplit(new RedoLogOffset(1L), RedoLogOffset.NO_STOPPING_OFFSET));
        RecordingChangeEventSource runningChangeEventSource = new RecordingChangeEventSource();
        fetchTask.setRunningChangeEventSource(runningChangeEventSource);
        RedoLogOffset earlier = lcrOffset("0000000008352fb60000000100000001");
        RedoLogOffset later = lcrOffset("0000000008363a3a0000000100000001");

        fetchTask.commitCurrentOffset(earlier);
        fetchTask.commitCurrentOffset(earlier);
        fetchTask.commitCurrentOffset(later);
        fetchTask.commitCurrentOffset(earlier);

        assertThat(runningChangeEventSource.committedOffsets).containsExactly(earlier, later);
    }

    private static StreamSplit streamSplit(RedoLogOffset start, RedoLogOffset end) {
        return new StreamSplit(
                StreamSplit.STREAM_SPLIT_ID, start, end, new ArrayList<>(), new HashMap<>(), 0);
    }

    private static RedoLogOffset lcrOffset(String lcrPosition) {
        HashMap<String, String> offset = new HashMap<>();
        offset.put(RedoLogOffset.LCR_POSITION_KEY, lcrPosition);
        offset.put("snapshot_scn", "null");
        offset.put("transaction_id", "null");
        return new RedoLogOffset(offset);
    }

    private static OracleConnectorConfig connectorConfig(String adapter) {
        Configuration.Builder builder =
                Configuration.create()
                        .with("database.server.name", "test")
                        .with("database.dbname", "ORCLCDB")
                        .with("database.hostname", "127.0.0.1")
                        .with("database.port", 1521)
                        .with("database.user", "flinkuser")
                        .with("database.password", "flinkpw");
        if (adapter != null) {
            builder.with("database.connection.adapter", adapter);
        }
        return new OracleConnectorConfig(builder.build());
    }

    private static class RecordingChangeEventSource
            implements OracleStreamFetchTask.RunningChangeEventSource {

        private final List<Offset> committedOffsets = new ArrayList<>();

        @Override
        public void stop() {}

        @Override
        public boolean commitOffset(Offset offset) {
            committedOffsets.add(offset);
            return true;
        }
    }
}
