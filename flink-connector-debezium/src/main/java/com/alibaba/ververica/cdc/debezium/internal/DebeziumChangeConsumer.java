/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.debezium.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A consumer that consumes change messages from {@link DebeziumEngine}.
 *
 * @param <T> The type of elements produced by the consumer.
 */
@Internal
public class DebeziumChangeConsumer<T> implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

	private static final Logger LOG = LoggerFactory.getLogger(DebeziumChangeConsumer.class);

	public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
	public static final String LAST_COMMIT_LSN_KEY = "lsn_commit";

	private final SourceFunction.SourceContext<T> sourceContext;

	/** The lock that guarantees that record emission and state updates are atomic,
	 * from the view of taking a checkpoint. */
	private final Object checkpointLock;

	/** The schema to convert from Debezium's messages into Flink's objects. */
	private final DebeziumDeserializationSchema<T> deserialization;

	/** A collector to emit records in batch (bundle). **/
	private final DebeziumCollector debeziumCollector;

	private final ErrorReporter errorReporter;

	private final DebeziumOffset debeziumOffset;

	private final DebeziumOffsetSerializer stateSerializer;

	private final String heartbeatTopicPrefix;

	private boolean isInDbSnapshotPhase;

	private boolean lockHold = false;

	private DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> currentCommitter;

	// ------------------------------------------------------------------------

	public DebeziumChangeConsumer(
			SourceFunction.SourceContext<T> sourceContext,
			DebeziumDeserializationSchema<T> deserialization,
			boolean isInDbSnapshotPhase,
			ErrorReporter errorReporter,
			String heartbeatTopicPrefix) {
		this.sourceContext = sourceContext;
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.deserialization = deserialization;
		this.isInDbSnapshotPhase = isInDbSnapshotPhase;
		this.heartbeatTopicPrefix = heartbeatTopicPrefix;
		this.debeziumCollector = new DebeziumCollector();
		this.errorReporter = errorReporter;
		this.debeziumOffset = new DebeziumOffset();
		this.stateSerializer = DebeziumOffsetSerializer.INSTANCE;
	}

	@Override
	public void handleBatch(
			List<ChangeEvent<SourceRecord, SourceRecord>> changeEvents,
			DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) throws InterruptedException {
		this.currentCommitter = committer;
		try {
			for (ChangeEvent<SourceRecord, SourceRecord> event : changeEvents) {
				SourceRecord record = event.value();
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

				if (isInDbSnapshotPhase) {
					if (!lockHold) {
						MemoryUtils.UNSAFE.monitorEnter(checkpointLock);
						lockHold = true;
						LOG.info("Database snapshot phase can't perform checkpoint, acquired Checkpoint lock.");
					}
					if (!isSnapshotRecord(record)) {
						MemoryUtils.UNSAFE.monitorExit(checkpointLock);
						isInDbSnapshotPhase = false;
						LOG.info("Received record from streaming binlog phase, released checkpoint lock.");
					}
				}

				// emit the actual records. this also updates offset state atomically
				emitRecordsUnderCheckpointLock(debeziumCollector.records, record.sourcePartition(), record.sourceOffset());
			}
		} catch (Exception e) {
			LOG.error("Error happens when consuming change messages.", e);
			errorReporter.reportError(e);
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

	private void emitRecordsUnderCheckpointLock(
			Queue<T> records,
			Map<String, ?> sourcePartition,
			Map<String, ?> sourceOffset) throws InterruptedException {
		if (isInDbSnapshotPhase) {
			// lockHolderThread holds the lock, don't need to hold it again
			emitRecords(records, sourcePartition, sourceOffset);
		} else {
			// emit the records, using the checkpoint lock to guarantee
			// atomicity of record emission and offset state update
			synchronized (checkpointLock) {
				emitRecords(records, sourcePartition, sourceOffset);
			}
		}
	}

	/**
	 * Emits a batch of records.
	 */
	private void emitRecords(
			Queue<T> records,
			Map<String, ?> sourcePartition,
			Map<String, ?> sourceOffset) {
		T record;
		while ((record = records.poll()) != null) {
			sourceContext.collect(record);
		}
		// update offset to state
		debeziumOffset.setSourcePartition(sourcePartition);
		debeziumOffset.setSourceOffset(sourceOffset);
	}

	/**
	 * Takes a snapshot of the Debezium Consumer state.
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

	@SuppressWarnings("unchecked")
	public void commitOffset(DebeziumOffset offset) throws InterruptedException {
		if (currentCommitter == null) {
			LOG.info("commitOffset() called on Debezium ChangeConsumer which doesn't receive records yet.");
			return;
		}

		// only the offset is used
		SourceRecord recordWrapper = new SourceRecord(
				offset.sourcePartition,
				adjustSourceOffset((Map<String, Object>) offset.sourceOffset),
				"DUMMY",
				Schema.BOOLEAN_SCHEMA,
				true);
		EmbeddedEngineChangeEvent<SourceRecord, SourceRecord> changeEvent = new EmbeddedEngineChangeEvent<>(
				null, recordWrapper, recordWrapper);
		currentCommitter.markProcessed(changeEvent);
		currentCommitter.markBatchFinished();
	}

	/**
	 * We have to adjust type of LSN values to Long, because it might be Integer after deserialization,
	 * however {@code io.debezium.connector.postgresql.PostgresStreamingChangeEventSource#commitOffset(java.util.Map)}
	 * requires Long.
	 */
	private Map<String, Object> adjustSourceOffset(Map<String, Object> sourceOffset) {
		if (sourceOffset.containsKey(LAST_COMPLETELY_PROCESSED_LSN_KEY)) {
			String value = sourceOffset.get(LAST_COMPLETELY_PROCESSED_LSN_KEY).toString();
			sourceOffset.put(LAST_COMPLETELY_PROCESSED_LSN_KEY, Long.parseLong(value));
		}
		if (sourceOffset.containsKey(LAST_COMMIT_LSN_KEY)) {
			String value = sourceOffset.get(LAST_COMMIT_LSN_KEY).toString();
			sourceOffset.put(LAST_COMMIT_LSN_KEY, Long.parseLong(value));
		}
		return sourceOffset;
	}

	private class DebeziumCollector implements Collector<T> {
		private final Queue<T> records = new ArrayDeque<>();

		@Override
		public void collect(T record) {
			records.add(record);
		}

		@Override
		public void close() {

		}
	}
}
