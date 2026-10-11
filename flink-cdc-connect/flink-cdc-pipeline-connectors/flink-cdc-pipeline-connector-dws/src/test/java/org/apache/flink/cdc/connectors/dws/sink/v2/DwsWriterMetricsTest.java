/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Metrics and safe-diagnostics contract for {@link DwsWriter}. */
class DwsWriterMetricsTest {

    private static final TableId TABLE = TableId.tableId("analytics", "metric_test");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("payload", DataTypes.STRING())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator GENERATOR =
            new BinaryRecordDataGenerator((RowType) SCHEMA.toRowDataType());

    @Test
    void distinguishesAcceptedRecordsFromFlushConfirmedRecords() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriterMetrics metrics = DwsWriterMetrics.testing();
        DwsWriter writer = writer(client, metrics);
        writer.write(new CreateTableEvent(TABLE, SCHEMA), null);

        writer.write(DataChangeEvent.insertEvent(TABLE, row(1, "accepted")), null);

        assertThat(metrics.acceptedRecords()).isOne();
        assertThat(metrics.writtenRecords()).isZero();
        assertThat(metrics.conservativeBufferedBytes()).isPositive();

        writer.flush(false);

        assertThat(metrics.writtenRecords()).isOne();
        assertThat(metrics.sentBytes()).isPositive();
        assertThat(metrics.conservativeBufferedBytes()).isZero();
    }

    @Test
    void countsOnlyDefiniteRecordFailures() throws Exception {
        RecordingClient client = new RecordingClient();
        client.writeFailure = new IOException("submit failed");
        DwsWriterMetrics metrics = DwsWriterMetrics.testing();
        DwsWriter writer = writer(client, metrics);
        writer.write(new CreateTableEvent(TABLE, SCHEMA), null);

        assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.insertEvent(TABLE, row(1, "secret-row")),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("submit failed");
        assertThat(metrics.acceptedRecords()).isZero();
        assertThat(metrics.writtenRecords()).isZero();
        assertThat(metrics.failedRecords()).isOne();
    }

    @Test
    void recordsSuccessfulFlushesButDoesNotClearOrConfirmOnFlushFailure() throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriterMetrics metrics = DwsWriterMetrics.testing();
        DwsWriter writer = writer(client, metrics);
        writer.write(new CreateTableEvent(TABLE, SCHEMA), null);
        writer.write(DataChangeEvent.insertEvent(TABLE, row(1, "pending")), null);
        long bufferedBeforeFailure = metrics.conservativeBufferedBytes();
        client.flushFailure = new IOException("flush failed");

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("flush failed");
        assertThat(metrics.flushCount()).isZero();
        assertThat(metrics.writtenRecords()).isZero();
        assertThat(metrics.conservativeBufferedBytes()).isEqualTo(bufferedBeforeFailure);

        client.flushFailure = null;
        writer.flush(false);
        assertThat(metrics.flushCount()).isOne();
        assertThat(metrics.lastFlushDurationMillis()).isNotNegative();
        assertThat(metrics.writtenRecords()).isOne();
    }

    @Test
    void exposesFirstAsyncFailureWithoutInventingFailedRecordCountOrLeakingSecrets()
            throws Exception {
        RecordingClient client = new RecordingClient();
        DwsWriterMetrics metrics = DwsWriterMetrics.testing();
        DwsWriter writer = writer(client, metrics);
        writer.recordAsyncFailure(new IOException("native worker failed"));
        writer.recordAsyncFailure(new IOException("later failure"));

        assertThat(metrics.hasFirstAsyncFailure()).isTrue();
        assertThat(metrics.failedRecords()).isZero();
        assertThat(writer.safeConfigurationSummary())
                .contains(
                        "writeMode=AUTO",
                        "retryMaxTimes=3",
                        "retryBase=PT1S",
                        "retryJitter=PT0.3S",
                        "taskTimeout=PT10M",
                        "statementTimeout=PT5M",
                        "bufferAccounting=conservative-estimate")
                .doesNotContain("jdbc:gaussdb", "metric-user", "metric-password", "secret-row");
    }

    private static DwsWriter writer(RecordingClient client, DwsWriterMetrics metrics) {
        DwsDataSinkConfig settings =
                DwsDataSinkConfig.builder()
                        .withUrl("jdbc:gaussdb://secret-host:8000/secret-database")
                        .withUsername("metric-user")
                        .withPassword("metric-password")
                        .withZoneId(ZoneId.of("UTC"))
                        .build();
        return new DwsWriter(settings, client, metrics, null, null);
    }

    private static RecordData row(int id, String payload) {
        return GENERATOR.generate(
                new Object[] {
                    id, org.apache.flink.cdc.common.data.binary.BinaryStringData.fromString(payload)
                });
    }

    private static final class RecordingClient implements DwsClientFacade {
        private IOException writeFailure;
        private IOException flushFailure;

        @Override
        public void write(String tableName, Map<String, Object> values) throws IOException {
            if (writeFailure != null) {
                throw writeFailure;
            }
        }

        @Override
        public void delete(String tableName, Map<String, Object> values) throws IOException {
            write(tableName, values);
        }

        @Override
        public void flush() throws IOException {
            if (flushFailure != null) {
                throw flushFailure;
            }
        }

        @Override
        public void close() {}
    }
}
