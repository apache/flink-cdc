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

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.oracle.source.OracleDialect;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class OracleSourceReaderCommitTest {

    @Test
    void testCommitOffsetImmediatelyWhenNewlyAddedTableScanIsDisabled() throws Exception {
        RecordingOracleDialect dialect = new RecordingOracleDialect();
        TestOracleSourceReader reader = createReader(false, dialect);
        RedoLogOffset offset = lcrOffset("0000000008352fb60000000100000001");

        reader.rememberCheckpointOffset(1L, offset);
        reader.notifyCheckpointComplete(1L);

        assertThat(dialect.committedOffsets).containsExactly(offset);
    }

    @Test
    void testDelayCommitOffsetUntilStreamSplitMetadataUpdatedForNewlyAddedTableScan()
            throws Exception {
        RecordingOracleDialect dialect = new RecordingOracleDialect();
        TestOracleSourceReader reader = createReader(true, dialect);
        RedoLogOffset blockedOffset = lcrOffset("0000000008352fb60000000100000001");
        RedoLogOffset allowedOffset = lcrOffset("0000000008363a3a0000000100000001");

        reader.rememberCheckpointOffset(1L, blockedOffset);
        reader.notifyCheckpointComplete(1L);
        assertThat(dialect.committedOffsets).isEmpty();

        reader.markStreamSplitMetadataUpdated();
        reader.rememberCheckpointOffset(2L, allowedOffset);
        reader.notifyCheckpointComplete(2L);

        assertThat(dialect.committedOffsets).containsExactly(allowedOffset);
    }

    private static TestOracleSourceReader createReader(
            boolean scanNewlyAddedTableEnabled, RecordingOracleDialect dialect) {
        TestingReaderContext readerContext = new TestingReaderContext();
        OracleSourceConfig sourceConfig = createSourceConfig(scanNewlyAddedTableEnabled);
        RedoLogOffsetFactory offsetFactory = new RedoLogOffsetFactory();
        SourceReaderMetrics sourceReaderMetrics =
                new SourceReaderMetrics(readerContext.metricGroup());
        RecordEmitter<SourceRecords, SourceRecord, SourceSplitState> recordEmitter =
                new IncrementalSourceRecordEmitter<>(
                        new ForwardDeserializeSchema(),
                        sourceReaderMetrics,
                        sourceConfig.isIncludeSchemaChanges(),
                        offsetFactory);
        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(readerContext);
        Supplier<IncrementalSourceSplitReader<JdbcSourceConfig>> splitReaderSupplier =
                () ->
                        new IncrementalSourceSplitReader<>(
                                readerContext.getIndexOfSubtask(),
                                dialect,
                                sourceConfig,
                                incrementalSourceReaderContext,
                                SnapshotPhaseHooks.empty());
        return new TestOracleSourceReader(
                new FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>>(),
                splitReaderSupplier,
                recordEmitter,
                readerContext.getConfiguration(),
                incrementalSourceReaderContext,
                sourceConfig,
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return offsetFactory;
                    }
                },
                dialect);
    }

    private static OracleSourceConfig createSourceConfig(boolean scanNewlyAddedTableEnabled) {
        return (OracleSourceConfig)
                new OracleSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList("ORCLCDB")
                        .tableList("DEBEZIUM.PRODUCTS")
                        .includeSchemaChanges(false)
                        .hostname("127.0.0.1")
                        .port(1521)
                        .username("flinkuser")
                        .password("flinkpw")
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                        .create(0);
    }

    private static RedoLogOffset lcrOffset(String lcrPosition) {
        HashMap<String, String> offset = new HashMap<>();
        offset.put(RedoLogOffset.LCR_POSITION_KEY, lcrPosition);
        offset.put("snapshot_scn", "null");
        offset.put("transaction_id", "null");
        return new RedoLogOffset(offset);
    }

    private static class TestOracleSourceReader extends OracleSourceReader {

        private TestOracleSourceReader(
                FutureCompletingBlockingQueue elementQueue,
                Supplier supplier,
                RecordEmitter recordEmitter,
                org.apache.flink.configuration.Configuration config,
                IncrementalSourceReaderContext incrementalSourceReaderContext,
                JdbcSourceConfig sourceConfig,
                SourceSplitSerializer sourceSplitSerializer,
                OracleDialect dialect) {
            super(
                    elementQueue,
                    supplier,
                    recordEmitter,
                    config,
                    incrementalSourceReaderContext,
                    sourceConfig,
                    sourceSplitSerializer,
                    dialect);
        }

        private void rememberCheckpointOffset(long checkpointId, Offset offset) {
            lastCheckpointOffsets.put(checkpointId, offset);
        }

        private void markStreamSplitMetadataUpdated() {
            updateStreamSplitFinishedSplitsSize(new LatestFinishedSplitsNumberEvent(0));
        }
    }

    private static class RecordingOracleDialect extends OracleDialect {

        private final List<Offset> committedOffsets = new ArrayList<>();

        @Override
        public void notifyCheckpointComplete(long checkpointId, Offset offset) {
            committedOffsets.add(offset);
        }
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }
}
