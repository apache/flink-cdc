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

package org.apache.flink.cdc.connectors.pgvector.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.metrics.Counter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/** Tests for {@link PgVectorSinkWriter}. */
class PgVectorSinkWriterTest {

    private static final TableId TABLE_ID = TableId.parse("public.docs");

    static {
        try {
            DriverManager.registerDriver(new RecordingDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    void testUpdateWithPrimaryKeyChangeDeletesOldKeyBeforeUpsert() throws Exception {
        RecordingDriver.clear();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("title", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        PgVectorDataSinkConfig config =
                new PgVectorDataSinkConfig(
                        RecordingDriver.URL,
                        "user",
                        "password",
                        "public",
                        true,
                        true,
                        false,
                        100,
                        Duration.ofSeconds(10),
                        0,
                        Duration.ZERO,
                        true,
                        false,
                        Collections.emptyMap(),
                        Collections.emptyMap());

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(config)) {
            writer.write(new CreateTableEvent(TABLE_ID, schema), new MockContext());
            writer.write(
                    DataChangeEvent.updateEvent(
                            TABLE_ID, GenericRecordData.of(1, null), GenericRecordData.of(2, null)),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(RecordingDriver.executedSql())
                .containsExactly(
                        "DELETE FROM \"public\".\"docs\" WHERE \"id\" = ?",
                        "INSERT INTO \"public\".\"docs\" (\"id\", \"title\") VALUES (?, ?) "
                                + "ON CONFLICT (\"id\") DO UPDATE SET "
                                + "\"title\" = EXCLUDED.\"title\"");
    }

    @Test
    void testFlushRetriesWithBackoffAndMetrics() throws Exception {
        RecordingDriver.clear();
        RecordingDriver.failNextExecuteBatch("08S01");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        PgVectorDataSinkConfig config =
                new PgVectorDataSinkConfig(
                        RecordingDriver.URL,
                        "user",
                        "password",
                        "public",
                        true,
                        true,
                        false,
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ofMillis(25),
                        true,
                        false,
                        Collections.emptyMap(),
                        Collections.emptyMap());
        TestingPgVectorSinkMetrics metrics = new TestingPgVectorSinkMetrics();

        long startMillis = System.currentTimeMillis();
        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(config, metrics)) {
            writer.write(new CreateTableEvent(TABLE_ID, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(TABLE_ID, GenericRecordData.of(1)),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(System.currentTimeMillis() - startMillis).isGreaterThanOrEqualTo(25);
        Assertions.assertThat(RecordingDriver.executedSql())
                .containsExactly(
                        "INSERT INTO \"public\".\"docs\" (\"id\") VALUES (?) "
                                + "ON CONFLICT (\"id\") DO NOTHING");
        Assertions.assertThat(RecordingDriver.connectionCount()).isGreaterThanOrEqualTo(2);
        Assertions.assertThat(metrics.pendingRows).isZero();
        Assertions.assertThat(metrics.successfulFlushRows.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.flushFailures.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.retries.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.reconnects.getCount()).isEqualTo(1);
        Assertions.assertThat(metrics.recordsOutErrors.getCount()).isZero();
    }

    @Test
    void testFinalFlushFailureRecordsOutputErrors() throws Exception {
        RecordingDriver.clear();
        RecordingDriver.failExecuteBatchTimes(4, "23505");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        PgVectorDataSinkConfig config =
                new PgVectorDataSinkConfig(
                        RecordingDriver.URL,
                        "user",
                        "password",
                        "public",
                        true,
                        true,
                        false,
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ZERO,
                        true,
                        false,
                        Collections.emptyMap(),
                        Collections.emptyMap());
        TestingPgVectorSinkMetrics metrics = new TestingPgVectorSinkMetrics();

        PgVectorSinkWriter writer = new PgVectorSinkWriter(config, metrics);
        writer.write(new CreateTableEvent(TABLE_ID, schema), new MockContext());
        writer.write(
                DataChangeEvent.insertEvent(TABLE_ID, GenericRecordData.of(1)), new MockContext());

        Assertions.assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to flush pgvector sink");
        Assertions.assertThatThrownBy(writer::close)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Simulated batch failure");

        Assertions.assertThat(RecordingDriver.executedSql()).isEmpty();
        Assertions.assertThat(metrics.flushFailures.getCount()).isEqualTo(4);
        Assertions.assertThat(metrics.retries.getCount()).isEqualTo(2);
        Assertions.assertThat(metrics.reconnects.getCount()).isZero();
        Assertions.assertThat(metrics.recordsOutErrors.getCount()).isEqualTo(2);
        Assertions.assertThat(metrics.pendingRows).isEqualTo(1);
    }

    private static class MockContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }

    private static class RecordingDriver implements Driver {

        private static final String URL = "jdbc:pgvector-recording:test";
        private static final List<String> EXECUTED_SQL = new ArrayList<>();
        private static int remainingExecuteBatchFailures;
        private static String nextFailureSqlState;
        private static int connectionCount;

        private static void clear() {
            EXECUTED_SQL.clear();
            remainingExecuteBatchFailures = 0;
            nextFailureSqlState = null;
            connectionCount = 0;
        }

        private static List<String> executedSql() {
            return new ArrayList<>(EXECUTED_SQL);
        }

        private static void failNextExecuteBatch(String sqlState) {
            failExecuteBatchTimes(1, sqlState);
        }

        private static void failExecuteBatchTimes(int times, String sqlState) {
            remainingExecuteBatchFailures = times;
            nextFailureSqlState = sqlState;
        }

        private static int connectionCount() {
            return connectionCount;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            connectionCount++;
            InvocationHandler handler =
                    (proxy, method, args) -> handleConnectionMethod(method, args);
            return (Connection)
                    Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {Connection.class},
                            handler);
        }

        @Override
        public boolean acceptsURL(String url) {
            return URL.equals(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        private Object handleConnectionMethod(Method method, Object[] args) {
            switch (method.getName()) {
                case "setAutoCommit":
                case "commit":
                case "rollback":
                case "close":
                    return null;
                case "isClosed":
                    return false;
                case "isValid":
                    return true;
                case "prepareStatement":
                    return createPreparedStatement((String) args[0]);
                default:
                    return defaultValue(method.getReturnType());
            }
        }

        private PreparedStatement createPreparedStatement(String sql) {
            InvocationHandler handler =
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "addBatch":
                            case "clearBatch":
                            case "close":
                                return null;
                            case "executeBatch":
                                if (remainingExecuteBatchFailures > 0) {
                                    remainingExecuteBatchFailures--;
                                    throw new SQLException(
                                            "Simulated batch failure.", nextFailureSqlState);
                                }
                                EXECUTED_SQL.add(sql);
                                return new int[] {1};
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    };
            return (PreparedStatement)
                    Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            handler);
        }

        private Object defaultValue(Class<?> returnType) {
            if (returnType == Boolean.TYPE) {
                return false;
            }
            if (returnType == Integer.TYPE) {
                return 0;
            }
            if (returnType == Long.TYPE) {
                return 0L;
            }
            if (returnType == Float.TYPE) {
                return 0.0f;
            }
            if (returnType == Double.TYPE) {
                return 0.0d;
            }
            return null;
        }
    }

    private static class TestingPgVectorSinkMetrics extends PgVectorSinkMetrics {

        private final Counter successfulFlushRows = new TestingCounter();
        private final Counter flushFailures = new TestingCounter();
        private final Counter retries = new TestingCounter();
        private final Counter reconnects = new TestingCounter();
        private final Counter recordsOutErrors = new TestingCounter();
        private long pendingRows;

        private TestingPgVectorSinkMetrics() {
            super(null);
        }

        @Override
        void setPendingRows(long pendingRows) {
            this.pendingRows = pendingRows;
        }

        @Override
        void recordSuccessfulFlush(long rows, long durationMillis) {
            successfulFlushRows.inc(rows);
            pendingRows = 0;
        }

        @Override
        void recordFlushFailure() {
            flushFailures.inc();
        }

        @Override
        void recordRecordsOutErrors(long rows) {
            recordsOutErrors.inc(rows);
        }

        @Override
        void recordRetry() {
            retries.inc();
        }

        @Override
        void recordReconnect() {
            reconnects.inc();
        }
    }

    private static class TestingCounter implements Counter {

        private long count;

        @Override
        public void inc() {
            inc(1);
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            dec(1);
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }
}
