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

package org.apache.flink.cdc.connectors.tidb.source.reader;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetFactory;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.meta.TiTimestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

/** Test for {@link TiDBTestBase}. */
public class TiDBStreamSplitReaderTest extends TiDBTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBStreamSplitReaderTest.class);
    private static final String databaseName = "customer";
    private static final String tableName = "customers";
    private static final String STREAM_SPLIT_ID = "stream-split";

    private static final int MAX_RETRY_TIMES = 600;

    private TiDBSourceConfig sourceConfig;
    private TiDBDialect tiDBDialect;
    private EventOffsetFactory cdcEventOffsetFactory;

    @BeforeEach
    public void before() {
        initializeTidbTable("customer");
        TiDBSourceConfigFactory tiDBSourceConfigFactory = new TiDBSourceConfigFactory();
        String pdHost = PD.getHost();
        String tikvHost = TIKV.getHost();
        String pdAddress = pdHost + ":" + PD.getMappedPort(PD_PORT_ORIGIN);

        String hostMapping = "pd0:" + pdHost + ";tikv0:" + tikvHost;

        TiConfiguration tiConfiguration =
                TiDBSourceOptions.getTiConfiguration(
                        pdAddress, hostMapping, Collections.emptyMap());
        tiDBSourceConfigFactory
                .pdAddresses(pdAddress)
                .hostMapping(hostMapping)
                .tiConfiguration(tiConfiguration);
        tiDBSourceConfigFactory.hostname(TIDB.getHost());
        tiDBSourceConfigFactory.port(TIDB.getMappedPort(TIDB_PORT));
        tiDBSourceConfigFactory.username(TiDBTestBase.TIDB_USER);
        tiDBSourceConfigFactory.password(TiDBTestBase.TIDB_PASSWORD);
        tiDBSourceConfigFactory.databaseList(this.databaseName);
        tiDBSourceConfigFactory.tableList(this.databaseName + "." + this.tableName);
        tiDBSourceConfigFactory.splitSize(10);
        tiDBSourceConfigFactory.skipSnapshotBackfill(true);
        tiDBSourceConfigFactory.scanNewlyAddedTableEnabled(true);
        this.sourceConfig = tiDBSourceConfigFactory.create(0);
        this.tiDBDialect = new TiDBDialect(sourceConfig);
        this.cdcEventOffsetFactory = new EventOffsetFactory();
    }

    @Test
    public void testStreamSplitReader() throws Exception {
        String tableId = databaseName + "." + tableName;
        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(new TestingReaderContext());
        IncrementalSourceSplitReader<JdbcSourceConfig> streamSplitReader =
                new IncrementalSourceSplitReader<>(
                        0,
                        tiDBDialect,
                        sourceConfig,
                        incrementalSourceReaderContext,
                        SnapshotPhaseHooks.empty());
        try {
            EventOffset startOffset = (EventOffset) tiDBDialect.displayCurrentOffset(sourceConfig);
            String[] insertDataSql =
                    new String[] {
                        "INSERT INTO "
                                + tableId
                                + " VALUES(112, 'user_12','Shanghai','123567891234')",
                        "INSERT INTO "
                                + tableId
                                + " VALUES(113, 'user_13','Shanghai','123567891234')",
                    };
            try (TiDBConnection tiDBConnection = tiDBDialect.openJdbcConnection()) {
                tiDBConnection.execute(insertDataSql);
                tiDBConnection.commit();
            }
            TableId tableIds = new TableId(databaseName, null, tableName);
            Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
            tableSchemas.put(tableIds, null);
            FinishedSnapshotSplitInfo finishedSnapshotSplitInfo =
                    new FinishedSnapshotSplitInfo(
                            tableIds,
                            STREAM_SPLIT_ID,
                            null,
                            null,
                            startOffset,
                            cdcEventOffsetFactory);
            StreamSplit streamSplit =
                    new StreamSplit(
                            STREAM_SPLIT_ID,
                            startOffset,
                            cdcEventOffsetFactory.createNoStoppingOffset(),
                            Collections.singletonList(finishedSnapshotSplitInfo),
                            tableSchemas,
                            1,
                            false,
                            true);
            Assertions.assertThat(streamSplitReader.canAssignNextSplit()).isTrue();
            streamSplitReader.handleSplitsChanges(new SplitsAddition<>(singletonList(streamSplit)));
            int retry = 0;
            int count = 0;
            while (retry++ < MAX_RETRY_TIMES) {
                ChangeEventRecords records = (ChangeEventRecords) streamSplitReader.fetch();
                if (records.nextSplit() != null) {
                    SourceRecords sourceRecords;
                    while ((sourceRecords = records.nextRecordFromSplit()) != null) {
                        Iterator<SourceRecord> iterator = sourceRecords.iterator();
                        while (iterator.hasNext()) {
                            Struct value = (Struct) iterator.next().value();
                            String opType = value.getString("op");
                            Assertions.assertThat(opType).isEqualTo("c");
                            Struct after = (Struct) value.get("after");
                            String name = after.getString("name");

                            Assertions.assertThat(name.contains("user")).isTrue();
                            if (++count >= insertDataSql.length) {
                                return;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            Assertions.fail("Timed out waiting for change events from stream split.");
        } catch (Exception e) {
            throw new AssertionError("Stream split read error.", e);
        } finally {
            streamSplitReader.close();
        }
    }

    @Test
    public void testCheckpointRestoreDoesNotReemitPreCheckpointRecords() throws Exception {
        EventOffset initialOffset = (EventOffset) tiDBDialect.displayCurrentOffset(sourceConfig);
        StreamSplit initialSplit = createStreamSplit(initialOffset);
        StreamSplit restoredSplit;

        IncrementalSourceSplitReader<JdbcSourceConfig> firstReader = createStreamSplitReader();
        try {
            assignSplit(firstReader, initialSplit);
            insertCustomer(112, "before_checkpoint");
            SourceRecord checkpointRecord = waitForRecord(firstReader, 112);
            EventOffset checkpointOffset = new EventOffset(checkpointRecord.sourceOffset());

            Assertions.assertThat(checkpointOffset.getTimestamp())
                    .isEqualTo(
                            String.valueOf(
                                    TiTimestamp.extractPhysical(
                                            Long.parseLong(checkpointOffset.getCommitVersion()))));
            restoredSplit = restoreCheckpoint(initialSplit, checkpointOffset);
        } finally {
            firstReader.close();
        }

        insertCustomer(113, "after_checkpoint");
        IncrementalSourceSplitReader<JdbcSourceConfig> restoredReader = createStreamSplitReader();
        try {
            assignSplit(restoredReader, restoredSplit);
            List<Integer> restoredIds = waitForRecordsThrough(restoredReader, 113);

            Assertions.assertThat(restoredIds).containsExactly(113);
        } finally {
            restoredReader.close();
        }
    }

    private IncrementalSourceSplitReader<JdbcSourceConfig> createStreamSplitReader() {
        IncrementalSourceReaderContext readerContext =
                new IncrementalSourceReaderContext(new TestingReaderContext());
        return new IncrementalSourceSplitReader<>(
                0, tiDBDialect, sourceConfig, readerContext, SnapshotPhaseHooks.empty());
    }

    private StreamSplit createStreamSplit(EventOffset startOffset) {
        TableId tableId = new TableId(databaseName, null, tableName);
        Map<TableId, TableChanges.TableChange> tableSchemas =
                tiDBDialect.discoverDataCollectionSchemas(sourceConfig);
        FinishedSnapshotSplitInfo finishedSnapshotSplitInfo =
                new FinishedSnapshotSplitInfo(
                        tableId, STREAM_SPLIT_ID, null, null, startOffset, cdcEventOffsetFactory);
        return new StreamSplit(
                STREAM_SPLIT_ID,
                startOffset,
                cdcEventOffsetFactory.createNoStoppingOffset(),
                Collections.singletonList(finishedSnapshotSplitInfo),
                tableSchemas,
                1,
                false,
                true);
    }

    private void assignSplit(
            IncrementalSourceSplitReader<JdbcSourceConfig> reader, StreamSplit streamSplit) {
        Assertions.assertThat(reader.canAssignNextSplit()).isTrue();
        reader.handleSplitsChanges(new SplitsAddition<>(singletonList(streamSplit)));
    }

    private void insertCustomer(int id, String name) throws Exception {
        String insertSql =
                String.format(
                        "INSERT INTO %s.%s VALUES(%d, '%s','Shanghai','123567891234')",
                        databaseName, tableName, id, name);
        try (TiDBConnection connection = tiDBDialect.openJdbcConnection()) {
            connection.execute(new String[] {insertSql});
            connection.commit();
        }
    }

    private SourceRecord waitForRecord(
            IncrementalSourceSplitReader<JdbcSourceConfig> reader, int expectedId)
            throws Exception {
        for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
            for (SourceRecord record : fetchRecords(reader)) {
                if (recordId(record) == expectedId) {
                    return record;
                }
            }
        }
        throw new AssertionError("Timed out waiting for record " + expectedId + '.');
    }

    private List<Integer> waitForRecordsThrough(
            IncrementalSourceSplitReader<JdbcSourceConfig> reader, int expectedLastId)
            throws Exception {
        List<Integer> ids = new ArrayList<>();
        for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
            for (SourceRecord record : fetchRecords(reader)) {
                int id = recordId(record);
                ids.add(id);
                if (id == expectedLastId) {
                    return ids;
                }
            }
        }
        throw new AssertionError("Timed out waiting for record " + expectedLastId + '.');
    }

    private List<SourceRecord> fetchRecords(IncrementalSourceSplitReader<JdbcSourceConfig> reader)
            throws Exception {
        List<SourceRecord> result = new ArrayList<>();
        ChangeEventRecords records = (ChangeEventRecords) reader.fetch();
        if (records.nextSplit() == null) {
            return result;
        }
        SourceRecords sourceRecords;
        while ((sourceRecords = records.nextRecordFromSplit()) != null) {
            sourceRecords.iterator().forEachRemaining(result::add);
        }
        return result;
    }

    private int recordId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct after = value.getStruct("after");
        return after.getInt32("id");
    }

    private StreamSplit restoreCheckpoint(StreamSplit split, EventOffset checkpointOffset)
            throws Exception {
        StreamSplitState checkpointState = new StreamSplitState(split);
        checkpointState.setStartingOffset(checkpointOffset);
        SourceSplitSerializer serializer =
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return cdcEventOffsetFactory;
                    }
                };
        byte[] checkpointBytes = serializer.serialize(checkpointState.toSourceSplit());
        SourceSplitBase restored = serializer.deserialize(serializer.getVersion(), checkpointBytes);
        return restored.asStreamSplit();
    }
}
