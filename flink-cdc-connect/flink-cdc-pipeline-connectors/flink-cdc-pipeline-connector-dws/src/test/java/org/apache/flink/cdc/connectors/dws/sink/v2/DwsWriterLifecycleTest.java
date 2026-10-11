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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Lifecycle and asynchronous failure tests for {@link DwsWriter}. */
class DwsWriterLifecycleTest {

    @Test
    void propagatesFirstAsyncFailureFromIdleHealthTimerThroughMailbox() throws Exception {
        RecordingClient client = new RecordingClient();
        CapturingMailbox mailbox = new CapturingMailbox();
        ManualProcessingTimeService processingTime = new ManualProcessingTimeService();
        DwsWriter writer = new DwsWriter(settings(), client, mailbox, processingTime);
        RuntimeException first = new RuntimeException("first async failure");

        writer.recordAsyncFailure(first);
        writer.recordAsyncFailure(new RuntimeException("later async failure"));
        processingTime.fire();

        assertThat(mailbox.failure).isInstanceOf(IOException.class);
        assertThat(mailbox.failure).hasCause(first);
        assertThat(processingTime.lastTimestamp).isEqualTo(1_000L);
    }

    @Test
    void checksAsyncFailureBeforeAcceptingMoreRecords() {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = new DwsWriter(settings(), client);
        RuntimeException failure = new RuntimeException("native worker failed");
        writer.recordAsyncFailure(failure);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasCause(failure);
        assertThat(client.flushCount).isZero();
    }

    @Test
    void snapshotDoesNotReturnMarkerWhenAsyncFailureAppearsDuringFlush() {
        RecordingClient client = new RecordingClient();
        RuntimeException failure = new RuntimeException("flush worker failed");
        client.failureAfterFlush = failure;
        DwsWriter writer = new DwsWriter(settings(), client);

        assertThatThrownBy(() -> writer.snapshotState(12L))
                .isInstanceOf(IOException.class)
                .hasCause(failure);
        assertThat(client.flushCount).isOne();
    }

    @Test
    void flushesAtSnapshotAndEndOfInput() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriter writer = new DwsWriter(settings(), client);

        writer.snapshotState(12L);
        writer.flush(true);

        assertThat(client.flushCount).isEqualTo(2);
    }

    @Test
    void closePreservesInitialFailureAndSuppressesCloseFailure() {
        RecordingClient client = new RecordingClient();
        client.closeFailure = new IOException("close failed");
        DwsWriter writer = new DwsWriter(settings(), client);
        RuntimeException first = new RuntimeException("first async failure");
        writer.recordAsyncFailure(first);

        assertThatThrownBy(writer::close)
                .isInstanceOf(IOException.class)
                .hasCause(first)
                .satisfies(
                        failure ->
                                assertThat(failure.getSuppressed()[0].getMessage())
                                        .isEqualTo("close failed"));
        assertThat(client.closed).isTrue();
    }

    private static DwsDataSinkConfig settings() {
        return DwsDataSinkConfig.builder()
                .withUrl("jdbc:gaussdb://localhost:8000/test")
                .withUsername("user")
                .withPassword("password")
                .withZoneId(ZoneId.of("UTC"))
                .build();
    }

    private static final class RecordingClient implements DwsClientFacade {
        private int flushCount;
        private boolean closed;
        private IOException closeFailure;
        private Throwable asyncFailure;
        private Throwable failureAfterFlush;

        @Override
        public void write(String tableName, Map<String, Object> values) {}

        @Override
        public void delete(String tableName, Map<String, Object> values) {}

        @Override
        public void flush() {
            flushCount++;
            asyncFailure = failureAfterFlush;
        }

        @Override
        public Throwable asyncFailure() {
            return asyncFailure;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class CapturingMailbox implements MailboxExecutor {
        private Throwable failure;

        @Override
        public void execute(
                MailOptions options,
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            try {
                command.run();
            } catch (Throwable t) {
                failure = t;
            }
        }

        @Override
        public void yield() {}

        @Override
        public boolean tryYield() {
            return false;
        }

        @Override
        public boolean shouldInterrupt() {
            return false;
        }
    }

    private static final class ManualProcessingTimeService implements ProcessingTimeService {
        private ProcessingTimeCallback callback;
        private long lastTimestamp;

        @Override
        public long getCurrentProcessingTime() {
            return 0L;
        }

        @Override
        public ScheduledFuture<?> registerTimer(
                long timestamp, ProcessingTimeCallback processingTimeCallback) {
            lastTimestamp = timestamp;
            callback = processingTimeCallback;
            return new CompletedScheduledFuture();
        }

        private void fire() throws Exception {
            callback.onProcessingTime(lastTimestamp);
        }
    }

    private static final class CompletedScheduledFuture implements ScheduledFuture<Object> {
        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed other) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            return null;
        }
    }
}
