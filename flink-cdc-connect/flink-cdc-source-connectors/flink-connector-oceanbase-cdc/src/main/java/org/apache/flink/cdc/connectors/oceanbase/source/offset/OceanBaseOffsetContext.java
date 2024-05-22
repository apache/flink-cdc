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

package org.apache.flink.cdc.connectors.oceanbase.source.offset;

import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** OceanBase offset context. */
public class OceanBaseOffsetContext implements OffsetContext {

    private final Schema sourceInfoSchema;
    private final OceanBaseSourceInfo sourceInfo;
    private boolean snapshotCompleted;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private String timestamp;
    private String commitVersion;
    private long transToSkip;
    private long eventsToSkip;

    public OceanBaseOffsetContext(
            boolean snapshot,
            boolean snapshotCompleted,
            TransactionContext transactionContext,
            IncrementalSnapshotContext<TableId> incrementalSnapshotContext,
            OceanBaseSourceInfo sourceInfo) {
        this.sourceInfoSchema = sourceInfo.schema();
        this.sourceInfo = sourceInfo;
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        } else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> offset = new HashMap<>();
        if (timestamp != null) {
            offset.put("timestamp", timestamp);
        }
        if (commitVersion != null) {
            offset.put("commit_version", commitVersion);
        }
        if (transToSkip != 0) {
            offset.put("trans", transToSkip);
        }
        if (eventsToSkip != 0) {
            offset.put("events", eventsToSkip);
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

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    public boolean isSnapshotCompleted() {
        return this.snapshotCompleted;
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
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public static OceanBaseOffsetContext initial(OceanBaseConnectorConfig config) {
        return new OceanBaseOffsetContext(
                false,
                false,
                new TransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>(),
                new OceanBaseSourceInfo(config));
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        this.sourceInfo.setSourceTime(timestamp);
        this.sourceInfo.tableEvent((TableId) tableId);
        this.eventsToSkip++;
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public void setCheckpoint(
            Instant timestamp, String commitVersion, long transToSkip, long eventsToSkip) {
        this.timestamp = Long.toString(timestamp.getEpochSecond());
        this.commitVersion = commitVersion;
        this.transToSkip = transToSkip;
        this.eventsToSkip = eventsToSkip;
    }

    public void setCommitVersion(Instant timestamp, String commitVersion) {
        this.setCheckpoint(timestamp, commitVersion, 0, 0);
    }

    public void commitVersionAccumulate() {
        transToSkip++;
    }

    public String getCommitVersion() {
        return commitVersion;
    }

    public void beginTransaction(String transactionId) {
        sourceInfo.beginTransaction(transactionId);
        eventsToSkip = 0;
    }

    public void commitTransaction() {
        sourceInfo.commitTransaction();
    }

    /** The OceanBase offset context loader. */
    public static class Loader implements OffsetContext.Loader<OceanBaseOffsetContext> {

        private final OceanBaseConnectorConfig connectorConfig;

        public Loader(OceanBaseConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public OceanBaseOffsetContext load(Map<String, ?> offset) {
            boolean snapshot = isTrue(offset.get("snapshot"));
            boolean snapshotCompleted = isTrue(offset.get("snapshot_completed"));

            IncrementalSnapshotContext<TableId> incrementalSnapshotContext =
                    SignalBasedIncrementalSnapshotContext.load(offset);

            final OceanBaseOffsetContext offsetContext =
                    new OceanBaseOffsetContext(
                            snapshot,
                            snapshotCompleted,
                            TransactionContext.load(offset),
                            incrementalSnapshotContext,
                            new OceanBaseSourceInfo(connectorConfig));

            String timestamp = (String) offset.get("timestamp");
            Long trans = (Long) offset.get("trans");
            Long events = (Long) offset.get("events");
            offsetContext.setCheckpoint(
                    timestamp == null
                            ? Instant.now()
                            : Instant.ofEpochSecond(Long.parseLong(timestamp)),
                    (String) offset.get("commit_version"),
                    trans == null ? 0 : trans,
                    events == null ? 0 : events);
            return offsetContext;
        }

        private boolean isTrue(Object obj) {
            return obj != null && Boolean.parseBoolean(obj.toString());
        }
    }
}
