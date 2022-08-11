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

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** Consume debezium change events. */
@Internal
public class DebeziumChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String LAST_COMMIT_LSN_KEY = "lsn_commit";
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumChangeConsumer.class);

    private final Handover handover;
    // keep the modification is visible to the source function
    private volatile RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> currentCommitter;

    public DebeziumChangeConsumer(Handover handover) {
        this.handover = handover;
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<SourceRecord, SourceRecord>> events,
            RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> recordCommitter) {
        try {
            currentCommitter = recordCommitter;
            handover.produce(events);
        } catch (Throwable e) {
            // Hold this exception in handover and trigger the fetcher to exit
            handover.reportError(e);
        }
    }

    @SuppressWarnings("unchecked")
    public void commitOffset(DebeziumOffset offset) throws InterruptedException {
        // Although the committer is read/write by multi-thread, the committer will be not changed
        // frequently.
        if (currentCommitter == null) {
            LOG.info(
                    "commitOffset() called on Debezium change consumer which doesn't receive records yet.");
            return;
        }

        // only the offset is used
        SourceRecord recordWrapper =
                new SourceRecord(
                        offset.sourcePartition,
                        adjustSourceOffset((Map<String, Object>) offset.sourceOffset),
                        "DUMMY",
                        Schema.BOOLEAN_SCHEMA,
                        true);
        EmbeddedEngineChangeEvent<SourceRecord, SourceRecord> changeEvent =
                new EmbeddedEngineChangeEvent<>(null, recordWrapper, recordWrapper);
        currentCommitter.markProcessed(changeEvent);
        currentCommitter.markBatchFinished();
    }

    /**
     * We have to adjust type of LSN values to Long, because it might be Integer after
     * deserialization, however {@code
     * io.debezium.connector.postgresql.PostgresStreamingChangeEventSource#commitOffset(java.util.Map)}
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
}
