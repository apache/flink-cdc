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

package org.apache.flink.cdc.runtime.operators.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.sink.FlushEventSinkWriter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class FlushEventSinkWritersTest {

    private static final TableId TABLE_ID = TableId.tableId("default", "sink_table");

    @Test
    void testFlushEventSinkWriterReceivesFlushEvent() throws Exception {
        FlushEvent flushEvent = createFlushEvent();
        RecordingFlushEventSinkWriter sinkWriter = new RecordingFlushEventSinkWriter();

        FlushEventSinkWriters.flush(sinkWriter, flushEvent);

        assertThat(sinkWriter.flushEvent).isSameAs(flushEvent);
        assertThat(sinkWriter.checkpointFlushCount).isZero();
    }

    @Test
    void testFallbackSinkWriterUsesCheckpointFlush() throws Exception {
        FlushEvent flushEvent = createFlushEvent();
        RecordingSinkWriter sinkWriter = new RecordingSinkWriter();

        FlushEventSinkWriters.flush(sinkWriter, flushEvent);

        assertThat(sinkWriter.checkpointFlushCount).isOne();
        assertThat(sinkWriter.endOfInput).isFalse();
    }

    private static FlushEvent createFlushEvent() {
        return new FlushEvent(
                0, Collections.singletonList(TABLE_ID), SchemaChangeEventType.ADD_COLUMN);
    }

    private static class RecordingSinkWriter implements SinkWriter<Event> {

        int checkpointFlushCount;
        Boolean endOfInput;

        @Override
        public void write(Event element, Context context)
                throws IOException, InterruptedException {}

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            checkpointFlushCount++;
            this.endOfInput = endOfInput;
        }

        @Override
        public void close() throws Exception {}
    }

    private static class RecordingFlushEventSinkWriter extends RecordingSinkWriter
            implements FlushEventSinkWriter {

        FlushEvent flushEvent;

        @Override
        public void flush(FlushEvent event) throws IOException, InterruptedException {
            this.flushEvent = event;
        }
    }
}
