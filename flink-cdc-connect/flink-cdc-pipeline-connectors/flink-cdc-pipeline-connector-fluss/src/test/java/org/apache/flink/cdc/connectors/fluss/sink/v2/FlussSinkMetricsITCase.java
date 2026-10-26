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

package org.apache.flink.cdc.connectors.fluss.sink.v2;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.InitContextAdapter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that both sink factories retain the framework's registered counters. */
@Timeout(60)
class FlussSinkMetricsITCase {

    @RegisterExtension
    private static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRegisteredCounters(boolean legacyContext) throws Exception {
        RecordingMetricGroup metrics = new RecordingMetricGroup();
        TaskMailboxImpl mailbox = new TaskMailboxImpl();
        TablePath tablePath = TablePath.of(BUILTIN_DATABASE, "sink_metrics_" + legacyContext);
        try (Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createTable(
                            tablePath,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .build())
                                    .distributedBy(1)
                                    .build(),
                            false)
                    .get();
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableInfo.getTableId());
            try (FlussSinkWriter<FlussEvent> writer =
                    createWriter(legacyContext, metrics, mailbox)) {
                assertThat(metrics.attempts)
                        .containsEntry("numRecordsOutErrors", 1)
                        .containsEntry("numRecordsSendErrors", 1)
                        .containsEntry("numRecordsSend", 1)
                        .containsEntry("numBytesSend", 1);
                assertThat(writer.metricGroup).isSameAs(metrics.sinkWriterMetricGroup);
                assertCounterIdentity(writer.metricGroup, metrics);

                writer.write(event(tablePath, tableInfo.getSchemaId()), null);
                writer.flush(false);
                assertThat(metrics.registeredCounter("numRecordsSend").getCount()).isEqualTo(1);
                assertThat(metrics.registeredCounter("numRecordsOutErrors").getCount()).isZero();
            } finally {
                admin.dropTable(tablePath, false).get();
            }
        } finally {
            mailbox.close();
            metrics.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void testAsyncFailureUpdatesRegisteredCounters(boolean legacyContext, boolean useMailbox)
            throws Exception {
        RecordingMetricGroup metrics = new RecordingMetricGroup();
        TaskMailboxImpl mailbox = new TaskMailboxImpl();
        try (FlussSinkWriter<FlussEvent> writer = createWriter(legacyContext, metrics, mailbox)) {
            writer.write(event(TablePath.of(BUILTIN_DATABASE, "missing_metrics_table"), 1), null);
            Mail errorMail = mailbox.take(TaskMailbox.MIN_PRIORITY);
            assertThat(metrics.registeredCounter("numRecordsOutErrors").getCount()).isZero();
            if (useMailbox) {
                assertThatThrownBy(errorMail::run)
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Fluss Writer send requests")
                        .rootCause()
                        .hasMessageContaining("missing_metrics_table")
                        .hasMessageContaining("does not exist");
            } else {
                assertThatThrownBy(() -> writer.flush(false))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Fluss Writer send requests")
                        .rootCause()
                        .hasMessageContaining("missing_metrics_table")
                        .hasMessageContaining("does not exist");
                errorMail.run();
            }
            assertThat(metrics.registeredCounter("numRecordsOutErrors").getCount()).isEqualTo(1);
            assertThat(metrics.registeredCounter("numRecordsSendErrors").getCount()).isEqualTo(1);
            assertThat(metrics.registeredCounter("numRecordsSend").getCount()).isEqualTo(1);
            assertCounterIdentity(writer.metricGroup, metrics);
        } finally {
            mailbox.close();
            metrics.close();
        }
    }

    private static void assertCounterIdentity(
            SinkWriterMetricGroup writerMetrics, RecordingMetricGroup metrics) {
        assertThat(writerMetrics.getNumRecordsOutErrorsCounter())
                .isSameAs(metrics.registeredCounter("numRecordsOutErrors"));
        assertThat(writerMetrics.getNumRecordsSendErrorsCounter())
                .isSameAs(metrics.registeredCounter("numRecordsSendErrors"));
        assertThat(writerMetrics.getNumRecordsSendCounter())
                .isSameAs(metrics.registeredCounter("numRecordsSend"))
                .isSameAs(
                        metrics.sinkWriterMetricGroup.getIOMetricGroup().getNumRecordsOutCounter());
        assertThat(writerMetrics.getNumBytesSendCounter())
                .isSameAs(metrics.registeredCounter("numBytesSend"))
                .isSameAs(metrics.sinkWriterMetricGroup.getIOMetricGroup().getNumBytesOutCounter());
    }

    private static FlussEvent event(TablePath tablePath, int schemaId) {
        return new FlussEvent(
                tablePath,
                Collections.singletonList(
                        new FlussRowWithOp(GenericRow.of(1), FlussOperationType.APPEND)),
                schemaId);
    }

    private static FlussSinkWriter<FlussEvent> createWriter(
            boolean legacyContext, RecordingMetricGroup metrics, TaskMailboxImpl mailbox)
            throws IOException {
        MailboxExecutor executor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);
        WriterInitContext context =
                (WriterInitContext)
                        Proxy.newProxyInstance(
                                FlussSinkMetricsITCase.class.getClassLoader(),
                                new Class<?>[] {WriterInitContext.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "metricGroup":
                                            return metrics.sinkWriterMetricGroup;
                                        case "getMailboxExecutor":
                                            return executor;
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });
        FlussSink<FlussEvent> sink =
                new FlussSink<>(
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        new FlussEventSerializer<FlussEvent>() {
                            @Override
                            public void open(Connection connection) {}

                            @Override
                            public FlussEvent serialize(FlussEvent event) {
                                return event;
                            }
                        });
        return (FlussSinkWriter<FlussEvent>)
                (legacyContext
                        ? sink.createWriter(new InitContextAdapter(context))
                        : sink.createWriter(context));
    }

    private static class RecordingMetricGroup extends GenericMetricGroup
            implements OperatorMetricGroup {
        private final Map<String, Integer> attempts = new HashMap<>();
        private final Map<String, Metric> registered;
        private final OperatorIOMetricGroup ioMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        private final InternalSinkWriterMetricGroup sinkWriterMetricGroup;

        private RecordingMetricGroup() {
            this(new ConcurrentHashMap<>());
        }

        private RecordingMetricGroup(Map<String, Metric> registered) {
            super(
                    TestingMetricRegistry.builder()
                            .setRegisterConsumer(
                                    (metric, name, group) -> {
                                        if (group instanceof RecordingMetricGroup) {
                                            registered.putIfAbsent(name, metric);
                                        }
                                    })
                            .build(),
                    null,
                    "sink");
            this.registered = registered;
            sinkWriterMetricGroup = InternalSinkWriterMetricGroup.wrap(this);
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            attempts.merge(name, 1, Integer::sum);
            return super.counter(name, counter);
        }

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            return ioMetricGroup;
        }

        private Counter registeredCounter(String name) {
            return (Counter) registered.get(name);
        }
    }
}
