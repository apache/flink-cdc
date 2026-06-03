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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

/** Tests for {@link TDengineEventWriter}. */
class TDengineEventWriterTest {

    @Test
    void testAutoFlushesWhenMaxRowsReached() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.config(
                                2,
                                900_000,
                                0,
                                Duration.ZERO,
                                "reject",
                                "upsert",
                                "reject",
                                "reject"),
                        TDengineSinkMetrics.NOOP,
                        ignored -> client);

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);
        writer.write(insert(1000L, "device-1"), null);
        writer.write(insert(2000L, "device-2"), null);

        Assertions.assertThat(client.sqls).hasSize(1);
        Assertions.assertThat(client.sqls.get(0))
                .contains("INSERT INTO `test_db`.`device_1` USING `test_db`.`metrics`")
                .contains("`test_db`.`device_2` USING `test_db`.`metrics`");
    }

    @Test
    void testSplitsFlushByMaxSqlBytes() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.config(
                                100, 150, 0, Duration.ZERO, "reject", "upsert", "reject", "reject"),
                        TDengineSinkMetrics.NOOP,
                        ignored -> client);

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);
        writer.write(insert(1000L, "device-1"), null);
        writer.write(insert(2000L, "device-2"), null);
        writer.flush(false);

        Assertions.assertThat(client.sqls).hasSize(2);
        Assertions.assertThat(client.sqls.get(0)).contains("device_1").doesNotContain("device_2");
        Assertions.assertThat(client.sqls.get(1)).contains("device_2").doesNotContain("device_1");
    }

    @Test
    void testRetriesTransientFlushFailure() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        client.remainingFailures = 1;
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.config(
                                100,
                                900_000,
                                1,
                                Duration.ZERO,
                                "reject",
                                "upsert",
                                "reject",
                                "reject"),
                        TDengineSinkMetrics.NOOP,
                        ignored -> client);

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);
        writer.write(insert(1000L, "device-1"), null);
        writer.flush(false);

        Assertions.assertThat(client.sqls).hasSize(1);
        Assertions.assertThat(client.closeCount).isEqualTo(1);
    }

    @Test
    void testPartialFlushRetriesOnlyRemainingRowsAcrossStables() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        client.failOnInvocation = 2;
        TableId secondTable = TableId.parse("inventory.metrics_b");
        TDengineDataSinkConfig config =
                TDengineTestUtils.configWithMappings(
                        java.util.Map.of(
                                TDengineTestUtils.TABLE_ID, "metrics_a", secondTable, "metrics_b"),
                        1);
        TDengineEventWriter writer =
                new TDengineEventWriter(config, TDengineSinkMetrics.NOOP, ignored -> client);

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);
        writer.write(new CreateTableEvent(secondTable, TDengineTestUtils.SCHEMA), null);
        writer.write(insert(TDengineTestUtils.TABLE_ID, 1000L, "device-1"), null);
        writer.write(insert(secondTable, 2000L, "device-2"), null);
        writer.flush(false);

        Assertions.assertThat(client.sqls).hasSize(2);
        Assertions.assertThat(client.sqls.get(0)).contains("metrics_a").doesNotContain("metrics_b");
        Assertions.assertThat(client.sqls.get(1)).contains("metrics_b").doesNotContain("metrics_a");
    }

    @Test
    void testPartialFlushRetriesOnlyRemainingRows() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        client.failOnInvocation = 2;
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.config(
                                100, 150, 1, Duration.ZERO, "reject", "upsert", "reject", "reject"),
                        TDengineSinkMetrics.NOOP,
                        ignored -> client);

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);
        writer.write(insert(1000L, "device-1"), null);
        writer.write(insert(2000L, "device-2"), null);
        writer.flush(false);

        Assertions.assertThat(client.sqls).hasSize(2);
        Assertions.assertThat(client.sqls.get(0)).contains("device_1").doesNotContain("device_2");
        Assertions.assertThat(client.sqls.get(1)).contains("device_2").doesNotContain("device_1");
    }

    @Test
    void testDoesNotRetryNonTransientFlushFailure() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        client.remainingFailures = 1;
        client.transientFailure = false;
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.config(
                                100,
                                900_000,
                                3,
                                Duration.ZERO,
                                "reject",
                                "upsert",
                                "reject",
                                "reject"),
                        TDengineSinkMetrics.NOOP,
                        ignored -> client);

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);
        writer.write(insert(1000L, "device-1"), null);

        Assertions.assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush TDengine sink");
        Assertions.assertThat(client.sqls).isEmpty();
        Assertions.assertThat(client.closeCount).isZero();
    }

    @Test
    void testRejectsDeleteByDefault() throws Exception {
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.defaultConfig(),
                        TDengineSinkMetrics.NOOP,
                        ignored -> new TDengineTestUtils.RecordingClient());

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);

        Assertions.assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.deleteEvent(
                                                TDengineTestUtils.TABLE_ID,
                                                TDengineTestUtils.record(
                                                        1000L, "device-1", "room-a", 12.5D)),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("DELETE");
    }

    @Test
    void testRejectsTimestampChangeByDefault() throws Exception {
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.defaultConfig(),
                        TDengineSinkMetrics.NOOP,
                        ignored -> new TDengineTestUtils.RecordingClient());

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);

        Assertions.assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.updateEvent(
                                                TDengineTestUtils.TABLE_ID,
                                                TDengineTestUtils.record(
                                                        1000L, "device-1", "room-a", 12.5D),
                                                TDengineTestUtils.record(
                                                        2000L, "device-1", "room-a", 13.5D)),
                                        null))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage(
                        "TDengine sink rejects UPDATE events that change timestamp.field by default.");
    }

    @Test
    void testRejectsSubtableChangeByDefault() throws Exception {
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.defaultConfig(),
                        TDengineSinkMetrics.NOOP,
                        ignored -> new TDengineTestUtils.RecordingClient());

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);

        Assertions.assertThatThrownBy(
                        () ->
                                writer.write(
                                        DataChangeEvent.updateEvent(
                                                TDengineTestUtils.TABLE_ID,
                                                TDengineTestUtils.record(
                                                        1000L, "device-1", "room-a", 12.5D),
                                                TDengineTestUtils.record(
                                                        1000L, "device-2", "room-a", 13.5D)),
                                        null))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage(
                        "TDengine sink rejects UPDATE events that change subtable.field by default.");
    }

    @Test
    void testIgnoresDeleteWhenConfigured() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        TDengineEventWriter writer =
                new TDengineEventWriter(
                        TDengineTestUtils.config(
                                100,
                                900_000,
                                0,
                                Duration.ZERO,
                                "ignore",
                                "upsert",
                                "reject",
                                "reject"),
                        TDengineSinkMetrics.NOOP,
                        ignored -> client);

        writer.write(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA), null);
        writer.write(
                DataChangeEvent.deleteEvent(
                        TDengineTestUtils.TABLE_ID,
                        TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D)),
                null);
        writer.flush(false);

        Assertions.assertThat(client.sqls).isEmpty();
    }

    private static DataChangeEvent insert(long ts, String deviceId) {
        return insert(TDengineTestUtils.TABLE_ID, ts, deviceId);
    }

    private static DataChangeEvent insert(TableId tableId, long ts, String deviceId) {
        return DataChangeEvent.insertEvent(
                tableId, TDengineTestUtils.record(ts, deviceId, "room-a", 12.5D));
    }
}
