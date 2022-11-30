/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.debezium.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A Handler that convert change messages from {@link DebeziumEngine} to data in Flink. Considering
 * Debezium in different mode has different strategies to hold the lock, e.g. snapshot, the handler
 * also needs different strategy. In snapshot phase, the handler needs to hold the lock until the
 * snapshot finishes. But in non-snapshot phase, the handler only needs to hold the lock when
 * emitting the records.
 *
 * @param <T> The type of elements produced by the handler.
 */
@Internal
public class DebeziumChangeFetcher<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumChangeFetcher.class);

    private final SourceFunction.SourceContext<T> sourceContext;

    /**
     * The lock that guarantees that record emission and state updates are atomic, from the view of
     * taking a checkpoint.
     */
    private final Object checkpointLock;

    /** The schema to convert from Debezium's messages into Flink's objects. */
    private final DebeziumDeserializationSchema<T> deserialization;

    /** A collector to emit records in batch (bundle). */
    private final DebeziumCollector debeziumCollector;

    private final DebeziumOffset debeziumOffset;

    private final DebeziumOffsetSerializer stateSerializer;

    private final String heartbeatTopicPrefix;

    private boolean isInDbSnapshotPhase;

    private final Handover handover;

    private volatile boolean isRunning = true;

    // ---------------------------------------------------------------------------------------
    // Metrics
    // ---------------------------------------------------------------------------------------

    /** Timestamp of change event. If the event is a snapshot event, the timestamp is 0L. */
    private volatile long messageTimestamp = 0L;

    /** The last record processing time. */
    private volatile long processTime = 0L;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /**
     * emitDelay = EmitTime - messageTimestamp, where the EmitTime is the time the record leaves the
     * source operator.
     */
    private volatile long emitDelay = 0L;

    // ------------------------------------------------------------------------

    public DebeziumChangeFetcher(
            SourceFunction.SourceContext<T> sourceContext,
            DebeziumDeserializationSchema<T> deserialization,
            boolean isInDbSnapshotPhase,
            String heartbeatTopicPrefix,
            Handover handover) {
        this.sourceContext = sourceContext;
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.deserialization = deserialization;
        this.isInDbSnapshotPhase = isInDbSnapshotPhase;
        this.heartbeatTopicPrefix = heartbeatTopicPrefix;
        this.debeziumCollector = new DebeziumCollector();
        this.debeziumOffset = new DebeziumOffset();
        this.stateSerializer = DebeziumOffsetSerializer.INSTANCE;
        this.handover = handover;
    }

    /**
     * Take a snapshot of the Debezium handler state.
     *
     * <p>Important: This method must be called under the checkpoint lock.
     */
    public byte[] snapshotCurrentState() throws Exception {
        // this method assumes that the checkpoint lock is held
        assert Thread.holdsLock(checkpointLock);
        if (debeziumOffset.sourceOffset == null || debeziumOffset.sourcePartition == null) {
            return null;
        }

        return stateSerializer.serialize(debeziumOffset);
    }

    /**
     * Process change messages from the {@link Handover} and collect the processed messages by
     * {@link Collector}.
     */
    public void runFetchLoop() throws Exception {
        try {
            // begin snapshot database phase
            if (isInDbSnapshotPhase) {
                List<ChangeEvent<SourceRecord, SourceRecord>> events = handover.pollNext();

                synchronized (checkpointLock) {
                    LOG.info(
                            "Database snapshot phase can't perform checkpoint, acquired Checkpoint lock.");
                    handleBatch(events);
                    while (isRunning && isInDbSnapshotPhase) {
                        handleBatch(handover.pollNext());
                    }
                }
                LOG.info("Received record from streaming binlog phase, released checkpoint lock.");
            }

            // begin streaming binlog phase
            while (isRunning) {
                // If the handover is closed or has errors, exit.
                // If there is no streaming phase, the handover will be closed by the engine.
                handleBatch(handover.pollNext());
            }
        } catch (Handover.ClosedException e) {
            // ignore
        } catch (RetriableException e) {
            // Retriable exception should be ignored by DebeziumChangeFetcher,
            // refer https://issues.redhat.com/browse/DBZ-2531 for more information.
            // Because Retriable exception is ignored by the DebeziumEngine and
            // the retry is handled in io.debezium.connector.common.BaseSourceTask.poll()
            LOG.info(
                    "Ignore the RetriableException, the underlying DebeziumEngine will restart automatically",
                    e);
        }
    }

    public void close() {
        isRunning = false;
        handover.close();
    }

    // ---------------------------------------------------------------------------------------
    // Metric getter
    // ---------------------------------------------------------------------------------------

    /**
     * The metric indicates delay from data generation to entry into the system.
     *
     * <p>Note: the metric is available during the binlog phase. Use 0 to indicate the metric is
     * unavailable.
     */
    public long getFetchDelay() {
        return fetchDelay;
    }

    /**
     * The metric indicates delay from data generation to leaving the source operator.
     *
     * <p>Note: the metric is available during the binlog phase. Use 0 to indicate the metric is
     * unavailable.
     */
    public long getEmitDelay() {
        return emitDelay;
    }

    public long getIdleTime() {
        return System.currentTimeMillis() - processTime;
    }

    // ---------------------------------------------------------------------------------------
    // Helper
    // ---------------------------------------------------------------------------------------

    private void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> changeEvents)
            throws Exception {
        if (CollectionUtils.isEmpty(changeEvents)) {
            return;
        }
        this.processTime = System.currentTimeMillis();

        for (ChangeEvent<SourceRecord, SourceRecord> event : changeEvents) {
            SourceRecord record = event.value();
            updateMessageTimestamp(record);
            fetchDelay = isInDbSnapshotPhase ? 0L : processTime - messageTimestamp;

            if (isHeartbeatEvent(record)) {
                // keep offset update
                synchronized (checkpointLock) {
                    debeziumOffset.setSourcePartition(record.sourcePartition());
                    debeziumOffset.setSourceOffset(record.sourceOffset());
                }
                // drop heartbeat events
                continue;
            }

            deserialization.deserialize(record, debeziumCollector);

            if (!isSnapshotRecord(record)) {
                LOG.debug("Snapshot phase finishes.");
                isInDbSnapshotPhase = false;
            }

            // emit the actual records. this also updates offset state atomically
            emitRecordsUnderCheckpointLock(
                    debeziumCollector.records, record.sourcePartition(), record.sourceOffset());
        }
    }

    private void emitRecordsUnderCheckpointLock(
            Queue<T> records, Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) {
        // Emit the records. Use the checkpoint lock to guarantee
        // atomicity of record emission and offset state update.
        // The synchronized checkpointLock is reentrant. It's safe to sync again in snapshot mode.
        synchronized (checkpointLock) {
            T record;
            while ((record = records.poll()) != null) {
                emitDelay =
                        isInDbSnapshotPhase ? 0L : System.currentTimeMillis() - messageTimestamp;
                sourceContext.collect(record);
            }
            // update offset to state
            debeziumOffset.setSourcePartition(sourcePartition);
            debeziumOffset.setSourceOffset(sourceOffset);
        }
    }

    private void updateMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.SOURCE) == null) {
            return;
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source.schema().field(Envelope.FieldName.TIMESTAMP) == null) {
            return;
        }

        Long tsMs = source.getInt64(Envelope.FieldName.TIMESTAMP);
        if (tsMs != null) {
            this.messageTimestamp = tsMs;
        }
    }

    private boolean isHeartbeatEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null && topic.startsWith(heartbeatTopicPrefix);
    }

    private boolean isSnapshotRecord(SourceRecord record) {
        Struct value = (Struct) record.value();
        if (value != null) {
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            SnapshotRecord snapshotRecord = SnapshotRecord.fromSource(source);
            // even if it is the last record of snapshot, i.e. SnapshotRecord.LAST
            // we can still recover from checkpoint and continue to read the binlog,
            // because the checkpoint contains binlog position
            return SnapshotRecord.TRUE == snapshotRecord;
        }
        return false;
    }

    // ---------------------------------------------------------------------------------------

    private class DebeziumCollector implements Collector<T> {

        private final Queue<T> records = new ArrayDeque<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void close() {}
    }
}
