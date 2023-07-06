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
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import com.ververica.cdc.debezium.internal.DebeziumChangeFetcher;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import com.ververica.cdc.debezium.internal.Handover;

import javax.annotation.Nullable;

import java.util.Properties;

/**
 * Wrapper class of DebeziumSourceFunction that removes duplicate insert documents during snapshot
 * phase. It is required for fullChangelog mode since there will be no ChangelogNormalize to
 * sanitize them.
 */
public class DebeziumDeduplicateSourceFunction<T> extends DebeziumSourceFunction<T> {

    /** State name of the consumer's history records state. */
    public static final String SNAPSHOT_DOCUMENT_RECORDS = "snapshot-document-records";

    public static final String LAST_SNAPSHOT_DOCUMENT_ID = "last-snapshot-id";

    /** State to store snapshot taken documents IDs. */
    private BroadcastState<String, String> snapshotDocumentState;

    private String lastSnapshotDocumentId;

    public DebeziumDeduplicateSourceFunction(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties,
            @Nullable DebeziumOffset specificOffset,
            Validator validator) {
        super(deserializer, properties, specificOffset, validator);
        lastSnapshotDocumentId = null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);

        OperatorStateStore stateStore = context.getOperatorStateStore();
        snapshotDocumentState =
                stateStore.getBroadcastState(
                        new MapStateDescriptor<>(
                                SNAPSHOT_DOCUMENT_RECORDS,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO));

        if (context.isRestored()) {
            restoreSnapshotHistory();
        }
    }

    private void restoreSnapshotHistory() {
        try {
            lastSnapshotDocumentId = snapshotDocumentState.get(LAST_SNAPSHOT_DOCUMENT_ID);
            if (lastSnapshotDocumentId != null) {
                LOG.info("Restored last snapshot document ID: " + lastSnapshotDocumentId);
            }
        } catch (Exception ignored) {
            lastSnapshotDocumentId = null;
        }
    }

    @Override
    protected DebeziumChangeFetcher<T> createChangeFetcher(
            SourceContext<T> sourceContext,
            DebeziumDeserializationSchema<T> deserialization,
            boolean isInDbSnapshotPhase,
            String heartbeatTopicPrefix,
            Handover handover) {
        return new DebeziumDeduplicateChangeFetcher<>(
                sourceContext,
                deserialization,
                isInDbSnapshotPhase,
                heartbeatTopicPrefix,
                handover,
                lastSnapshotDocumentId,
                snapshotDocumentState);
    }
}
