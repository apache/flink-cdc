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

package org.apache.flink.cdc.connectors.postgres.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.base.source.meta.events.StreamSplitAssignedEvent;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.events.OffsetCommitAckEvent;
import org.apache.flink.cdc.connectors.postgres.source.events.OffsetCommitEvent;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;

import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigning;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/**
 * The Postgres source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Internal
public class PostgresSourceEnumerator extends IncrementalSourceEnumerator {

    private final PostgresDialect postgresDialect;
    private final PostgresSourceConfig sourceConfig;
    private volatile boolean receiveOffsetCommitAck = false;

    public PostgresSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            PostgresSourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            PostgresDialect postgresDialect,
            Boundedness boundedness) {
        super(context, sourceConfig, splitAssigner, boundedness);
        this.postgresDialect = postgresDialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void start() {
        createSlotForGlobalStreamSplit();
        super.start();
    }

    @Override
    protected void assignSplits() {
        // if scan newly added table is enable, can not assign new added table's snapshot splits
        // until source reader doesn't commit offset.
        if (sourceConfig.isScanNewlyAddedTableEnabled()
                && streamSplitTaskId != null
                && !receiveOffsetCommitAck
                && isNewlyAddedAssigning(splitAssigner.getAssignerStatus())) {
            // just return here, the reader has been put into readersAwaitingSplit, will be assigned
            // split again later
            return;
        }
        super.assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof StreamSplitAssignedEvent && this.receiveOffsetCommitAck) {
            this.receiveOffsetCommitAck = false;
        }
        if (sourceEvent instanceof OffsetCommitAckEvent) {
            if (streamSplitTaskId != null && streamSplitTaskId == subtaskId) {
                this.receiveOffsetCommitAck = true;
            } else {
                throw new RuntimeException("Receive SyncAssignStatusAck from wrong subtask");
            }
        } else {
            super.handleSourceEvent(subtaskId, sourceEvent);
        }
    }

    @Override
    protected void syncWithReaders(int[] subtaskIds, Throwable t) {
        super.syncWithReaders(subtaskIds, t);
        // if scan newly added table is enable, postgres enumerator will send its OffsetCommitEvent
        // to tell reader whether to start offset commit.
        if (!receiveOffsetCommitAck
                && sourceConfig.isScanNewlyAddedTableEnabled()
                && streamSplitTaskId != null) {
            AssignerStatus assignerStatus = splitAssigner.getAssignerStatus();
            context.sendEventToSourceReader(
                    streamSplitTaskId,
                    new OffsetCommitEvent(
                            !isNewlyAddedAssigning(assignerStatus)
                                    && !isNewlyAddedAssigningSnapshotFinished(assignerStatus)));
        }
    }

    /**
     * Create slot for the unique global stream split.
     *
     * <p>Currently all startup modes need read the stream split. We need open the slot before
     * reading the globalStreamSplit to catch all data changes.
     */
    private void createSlotForGlobalStreamSplit() {
        try (PostgresConnection connection = postgresDialect.openJdbcConnection()) {
            SlotState slotInfo =
                    connection.getReplicationSlotState(
                            postgresDialect.getSlotName(), postgresDialect.getPluginName());
            // skip creating the replication slot when the slot exists.
            if (slotInfo != null) {
                return;
            }
            PostgresReplicationConnection replicationConnection =
                    postgresDialect.openPostgresReplicationConnection(connection);
            replicationConnection.createReplicationSlot();
            replicationConnection.close(false);

        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Fail to get or create slot for global stream split, the slot name is %s. Due to: ",
                            postgresDialect.getSlotName()),
                    t);
        }
    }
}
