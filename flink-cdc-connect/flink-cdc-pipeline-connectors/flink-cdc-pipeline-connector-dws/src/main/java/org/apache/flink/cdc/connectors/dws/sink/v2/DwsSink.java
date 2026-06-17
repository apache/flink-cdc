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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import java.time.ZoneId;
import java.util.Collection;
import java.util.UUID;

/** A SinkV2 DWS sink with staging-table based application-level two-phase commit. */
public class DwsSink
        implements TwoPhaseCommittingSink<Event, DwsCommittable>,
                SupportsWriterState<Event, DwsWriterState> {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_SCHEMA = "public";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final ZoneId zoneId;
    private final boolean caseSensitive;
    private final String defaultSchema;
    private final boolean enableDelete;

    private String jobId;

    public DwsSink(
            String jdbcUrl,
            String username,
            String password,
            ZoneId zoneId,
            boolean caseSensitive,
            String defaultSchema,
            boolean enableDelete) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.zoneId = zoneId;
        this.caseSensitive = caseSensitive;
        this.defaultSchema = normalizeDefaultSchema(defaultSchema);
        this.enableDelete = enableDelete;
        this.jobId = "dws-" + UUID.randomUUID();
    }

    @Deprecated
    @Override
    public SinkWriter<Event> createWriter(Sink.InitContext context) {
        long lastCheckpointId =
                context.getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        return createWriter(context.getTaskInfo().getIndexOfThisSubtask(), lastCheckpointId);
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) {
        long lastCheckpointId =
                context.getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        return createWriter(context.getTaskInfo().getIndexOfThisSubtask(), lastCheckpointId);
    }

    @Override
    public StatefulSinkWriter<Event, DwsWriterState> restoreWriter(
            WriterInitContext context, Collection<DwsWriterState> writerStates) {
        long lastCheckpointId =
                context.getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        if (writerStates != null && !writerStates.isEmpty()) {
            jobId = writerStates.iterator().next().getJobId();
        }
        return createWriter(context.getTaskInfo().getIndexOfThisSubtask(), lastCheckpointId);
    }

    @Override
    public Committer<DwsCommittable> createCommitter(CommitterInitContext context) {
        return createCommitter();
    }

    @Override
    public Committer<DwsCommittable> createCommitter() {
        return new DwsCommitter(jdbcUrl, username, password, defaultSchema, caseSensitive);
    }

    @Override
    public SimpleVersionedSerializer<DwsCommittable> getCommittableSerializer() {
        return new DwsCommittableSerializer();
    }

    public SimpleVersionedSerializer<DwsCommittable> getWriteResultSerializer() {
        return getCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DwsWriterState> getWriterStateSerializer() {
        return new DwsWriterStateSerializer();
    }

    private DwsWriter createWriter(int subtaskId, long lastCheckpointId) {
        return new DwsWriter(
                jdbcUrl,
                username,
                password,
                zoneId,
                caseSensitive,
                defaultSchema,
                enableDelete,
                jobId,
                subtaskId,
                lastCheckpointId);
    }

    private static String normalizeDefaultSchema(String defaultSchema) {
        if (defaultSchema == null || defaultSchema.trim().isEmpty()) {
            return DEFAULT_SCHEMA;
        }
        return defaultSchema.trim();
    }
}
