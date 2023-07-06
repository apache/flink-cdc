/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mongodb.source.deduplicate;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.internal.DebeziumChangeFetcher;
import com.ververica.cdc.debezium.internal.Handover;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.ververica.cdc.connectors.mongodb.source.deduplicate.DebeziumDeduplicateSourceFunction.LAST_SNAPSHOT_DOCUMENT_ID;

/**
 * Wrapper class of DebeziumChangeFetcher that removes duplicate insert * documents during snapshot
 * phase. It is required for fullChangelog mode * since there will be no ChangelogNormalize to
 * sanitize them.
 */
public class DebeziumDeduplicateChangeFetcher<T> extends DebeziumChangeFetcher<T> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DebeziumDeduplicateChangeFetcher.class);

    private final String lastSnapshotDocumentId;
    private final BroadcastState<String, String> snapshotDocumentState;

    private boolean isInSnapshotPhase = true;
    private boolean isInSnapshotRecoverPhase = false;

    public DebeziumDeduplicateChangeFetcher(
            SourceFunction.SourceContext<T> sourceContext,
            DebeziumDeserializationSchema<T> deserialization,
            boolean isInDbSnapshotPhase,
            String heartbeatTopicPrefix,
            Handover handover,
            String lastSnapshotDocumentId,
            BroadcastState<String, String> snapshotDocumentState) {
        super(sourceContext, deserialization, isInDbSnapshotPhase, heartbeatTopicPrefix, handover);
        this.lastSnapshotDocumentId = lastSnapshotDocumentId;
        this.snapshotDocumentState = snapshotDocumentState;
        if (lastSnapshotDocumentId != null) {
            // we've just recovered from a snapshot failure
            LOG.info("Trying to restore snapshot from " + lastSnapshotDocumentId);
            this.isInSnapshotRecoverPhase = true;
        }
    }

    private static String extractSnapshotIdFromRecord(SourceRecord record) {
        Object val = record.value();
        if (!(val instanceof Struct)) {
            return null;
        }
        Struct value = (Struct) val;
        if (Objects.equals(value.getString("operationType"), "insert")) {
            return value.getString("_id");
        } else {
            return null;
        }
    }

    @Override
    protected void deserializeRecord(SourceRecord record) throws Exception {
        if (isInSnapshotPhase) {
            if (isSnapshotRecord(record)) {
                // check if we're still in snapshot stage
                String documentId = extractSnapshotIdFromRecord(record);
                if (isInSnapshotRecoverPhase) {
                    if (Objects.equals(documentId, lastSnapshotDocumentId)) {
                        // if we've caught up last snapshot progress, clear recover state
                        LOG.info(
                                "Snapshot failure recover caught up last progress "
                                        + lastSnapshotDocumentId);
                        isInSnapshotRecoverPhase = false;
                    }
                    // in snapshot failure recover phase, we shouldn't emit records
                    return;
                } else {
                    // or, we should update current snapshot progress
                    snapshotDocumentState.put(LAST_SNAPSHOT_DOCUMENT_ID, documentId);
                }
            } else {
                // snapshot phase is over, clear all states
                isInSnapshotPhase = false;
                isInSnapshotRecoverPhase = false;
            }
        }

        super.deserializeRecord(record);
    }

    private boolean isSnapshotRecord(SourceRecord record) {
        Struct value = (Struct) record.value();
        if (value != null) {
            Struct struct = value.getStruct("source");
            if (struct != null) {
                return Objects.equals(struct.getString("snapshot"), "true");
            }
        }
        return false;
    }
}
