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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DwsSink}. */
class DwsSinkTest {

    @Test
    void testCreateWriterSnapshotsGeneratedJobId() throws Exception {
        DwsSink sink = createSink();

        SinkWriter<Event> writer = sink.createWriter(new MockWriterInitContext(1));
        try {
            assertThat(((DwsWriter) writer).snapshotState(1L))
                    .singleElement()
                    .satisfies(state -> assertThat(state.getJobId()).startsWith("dws-"));
        } finally {
            writer.close();
        }
    }

    @Test
    void testRestoreWriterUsesRestoredJobId() throws Exception {
        DwsSink sink = createSink();

        StatefulSinkWriter<Event, DwsWriterState> writer =
                sink.restoreWriter(
                        new MockWriterInitContext(2, OptionalLong.of(100L)),
                        Collections.singletonList(new DwsWriterState("restored-job")));
        try {
            assertThat(writer.snapshotState(101L))
                    .singleElement()
                    .satisfies(state -> assertThat(state.getJobId()).isEqualTo("restored-job"));
        } finally {
            writer.close();
        }
    }

    @Test
    void testCreateCommitterAndSerializers() {
        DwsSink sink = createSink();

        assertThat(sink.createCommitter()).isInstanceOf(DwsCommitter.class);
        assertThat(sink.getCommittableSerializer()).isInstanceOf(DwsCommittableSerializer.class);
        assertThat(sink.getWriteResultSerializer()).isInstanceOf(DwsCommittableSerializer.class);
        assertThat(sink.getWriterStateSerializer()).isInstanceOf(DwsWriterStateSerializer.class);
    }

    private static DwsSink createSink() {
        return new DwsSink(
                "jdbc:gaussdb://localhost:8000/test",
                "user",
                "password",
                ZoneId.of("UTC"),
                false,
                "ods",
                true);
    }

    private static class MockWriterInitContext
            implements WriterInitContext, SerializationSchema.InitializationContext {

        private final int subtaskId;
        private final OptionalLong restoredCheckpointId;

        private MockWriterInitContext(int subtaskId) {
            this(subtaskId, OptionalLong.empty());
        }

        private MockWriterInitContext(int subtaskId, OptionalLong restoredCheckpointId) {
            this.subtaskId = subtaskId;
            this.restoredCheckpointId = restoredCheckpointId;
        }

        @Override
        public org.apache.flink.util.UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return null;
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return null;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return null;
        }

        @Override
        public MetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return restoredCheckpointId;
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return this;
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return false;
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        @Override
        public JobInfo getJobInfo() {
            return null;
        }

        @Override
        public TaskInfo getTaskInfo() {
            return new TaskInfoImpl("dws-sink", 4, subtaskId, 4, 0);
        }
    }
}
