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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
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

	private final SourceFunction.SourceContext<T> sourceContext;

	/** The lock that guarantees that record emission and state updates are atomic,
	 * from the view of taking a checkpoint. */
	private final Object checkpointLock;

	/** The schema to convert from Debezium's messages into Flink's objects. */
	private final DebeziumDeserializationSchema<T> deserialization;

	/** A collector to emit records in batch (bundle). **/
	private final DebeziumCollector debeziumCollector;

	private final ErrorReporter errorReporter;

	private final DebeziumState debeziumState;

	private final DebeziumStateSerializer stateSerializer;

	private boolean isInDbSnapshotPhase;

	// ------------------------------------------------------------------------

	public DebeziumChangeConsumer(
			SourceFunction.SourceContext<T> sourceContext,
			DebeziumDeserializationSchema<T> deserialization,
			boolean isInDbSnapshotPhase,
			ErrorReporter errorReporter) {
		this.sourceContext = sourceContext;
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.deserialization = deserialization;
		this.isInDbSnapshotPhase = isInDbSnapshotPhase;
		this.debeziumCollector = new DebeziumCollector();
		this.errorReporter = errorReporter;
		this.debeziumState = new DebeziumState();
		this.stateSerializer = new DebeziumStateSerializer();
	}

	@Override
	public void handleBatch(
			List<ChangeEvent<SourceRecord, SourceRecord>> changeEvents,
			DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) throws InterruptedException {
		try {
			int numOfSnapshots = handleSnapshots(changeEvents);

			for(int i = numOfSnapshots; i < changeEvents.size(); i++) {
				SourceRecord record = changeEvents.get(i).value();
				deserialization.deserialize(record, debeziumCollector);
				synchronized (checkpointLock) {
					emitRecords(debeziumCollector.records, record.sourcePartition(), record.sourceOffset());
				}
			}
		} catch (Exception e) {
			LOG.error("Error happened when consuming change messages.", e);
			errorReporter.reportError(e);
		}
	}

	private int handleSnapshots(List<ChangeEvent<SourceRecord, SourceRecord>> changeEvents) throws Exception {
		if (!isInDbSnapshotPhase) {
			LOG.debug("Not in Database snapshot phase, handling 0 snapshots");
			return 0;
		}

		int numOfSnapshots;
		synchronized (checkpointLock) {
			LOG.info("Database snapshot phase can't perform checkpoint, acquired Checkpoint lock.");
			for(numOfSnapshots = 0; numOfSnapshots < changeEvents.size() && isInDbSnapshotPhase; numOfSnapshots++) {
				SourceRecord record = changeEvents.get(numOfSnapshots).value();
				deserialization.deserialize(record, debeziumCollector);

				if (!isSnapshotRecord(record)) {
					isInDbSnapshotPhase = false;
					LOG.info("Received record from streaming binlog phase, releasing checkpoint lock.");
				}

				emitRecords(debeziumCollector.records, record.sourcePartition(), record.sourceOffset());
			}
		}

		LOG.info("Handled {} Database snapshots records", numOfSnapshots);
		return numOfSnapshots;
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
		debeziumState.setSourcePartition(sourcePartition);
		debeziumState.setSourceOffset(sourceOffset);
	}

	/**
	 * Takes a snapshot of the Debezium Consumer state.
	 *
	 * <p>Important: This method must be called under the checkpoint lock.
	 */
	public byte[] snapshotCurrentState() throws Exception {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);
		if (debeziumState.sourceOffset == null || debeziumState.sourcePartition == null) {
			return null;
		}

		return stateSerializer.serialize(debeziumState);
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
