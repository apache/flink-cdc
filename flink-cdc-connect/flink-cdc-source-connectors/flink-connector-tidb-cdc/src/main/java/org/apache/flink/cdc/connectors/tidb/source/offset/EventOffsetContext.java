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

package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.tikv.common.meta.TiTimestamp;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset.COMMIT_VERSION_KEY;
import static org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset.TIMESTAMP_KEY;

public class EventOffsetContext implements OffsetContext {
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final TiDBSourceInfo sourceInfo;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private boolean snapshotCompleted;
    private String commitVersion;
    private String timestamp;

    public EventOffsetContext(
            boolean snapshot,
            boolean snapshotCompleted,
            TransactionContext transactionContext,
            IncrementalSnapshotContext<TableId> incrementalSnapshotContext,
            TiDBSourceInfo sourceInfo) {
        this.sourceInfo = sourceInfo;
        this.sourceInfoSchema = sourceInfo.schema();
        this.snapshotCompleted = snapshotCompleted;

        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        } else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    public static EventOffsetContext initial(TiDBConnectorConfig config) {
        return new EventOffsetContext(
                false,
                false,
                new TransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>(),
                new TiDBSourceInfo(config));
    }

    @Override
    public Map<String, ?> getOffset() {
        HashMap<String, Object> offset = new HashMap<>();
        if (timestamp != null) {
            offset.put(TIMESTAMP_KEY, timestamp);
        }

        if (commitVersion != null) {
            offset.put(COMMIT_VERSION_KEY, commitVersion);
        }
        if (sourceInfo.isSnapshot()) {
            if (!snapshotCompleted) {
                offset.put(AbstractSourceInfo.SNAPSHOT_KEY, true);
            }
            return offset;
        } else {
            return incrementalSnapshotContext.store(transactionContext.store(offset));
        }
    }

    public void databaseEvent(String database, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.databaseEvent(database);
        sourceInfo.tableEvent((TableId) null);
    }

    public void tableEvent(String database, Set<TableId> tableIds, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.databaseEvent(database);
        sourceInfo.tableEvent(tableIds);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent((TableId) collectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public void setCheckpoint(Instant timestamp, String commitVersion) {
        this.timestamp = String.valueOf(timestamp.toEpochMilli());
        if (commitVersion == null) {
            commitVersion =
                    String.valueOf(new TiTimestamp(timestamp.toEpochMilli(), 0).getVersion());
        }
        this.commitVersion = commitVersion;
    }

    public static class Loader implements OffsetContext.Loader<EventOffsetContext> {

        private final TiDBConnectorConfig connectorConfig;

        public Loader(TiDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @SuppressWarnings("unchecked")
        @Override
        public EventOffsetContext load(Map<String, ?> offset) {
            boolean snapshot =
                    Boolean.TRUE.equals(offset.get(TiDBSourceInfo.SNAPSHOT_KEY))
                            || "true".equals(offset.get(TiDBSourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted =
                    Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY))
                            || "true".equals(offset.get(SNAPSHOT_COMPLETED_KEY));
            IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
            if (connectorConfig.isReadOnlyConnection()) {
                incrementalSnapshotContext = MySqlReadOnlyIncrementalSnapshotContext.load(offset);
            } else {
                incrementalSnapshotContext = SignalBasedIncrementalSnapshotContext.load(offset);
            }
            final EventOffsetContext offsetContext =
                    new EventOffsetContext(
                            snapshot,
                            snapshotCompleted,
                            TransactionContext.load(offset),
                            incrementalSnapshotContext,
                            new TiDBSourceInfo(connectorConfig));
            String timestamp = (String) offset.get(TIMESTAMP_KEY);
            offsetContext.setCheckpoint(
                    timestamp == null
                            ? Instant.now()
                            : Instant.ofEpochMilli(Long.parseLong(timestamp)),
                    (String) offset.get(COMMIT_VERSION_KEY));
            return offsetContext;
        }
    }
}
