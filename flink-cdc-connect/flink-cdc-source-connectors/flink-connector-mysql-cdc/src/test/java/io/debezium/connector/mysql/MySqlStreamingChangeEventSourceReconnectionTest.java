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

package io.debezium.connector.mysql;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for MySQL binlog reconnection behavior in MySqlStreamingChangeEventSource. */
class MySqlStreamingChangeEventSourceReconnectionTest {

    @Mock private MySqlConnectorConfig connectorConfig;
    @Mock private MySqlConnection connection;
    @Mock private EventDispatcher<MySqlPartition, TableId> eventDispatcher;
    @Mock private ErrorHandler errorHandler;
    @Mock private MySqlTaskContext taskContext;
    @Mock private BinaryLogClient binaryLogClient;
    @Mock private MySqlStreamingChangeEventSourceMetrics metrics;
    @Mock private MySqlOffsetContext offsetContext;
    @Mock private MySqlDatabaseSchema schema;
    @Mock private SourceInfo sourceInfo;

    private TestClock testClock;
    private MySqlStreamingChangeEventSource streamingSource;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        testClock = new TestClock();

        // Mock basic connector config
        when(connectorConfig.getSnapshotMode()).thenReturn(SnapshotMode.INITIAL);
        when(connectorConfig.getConnectionTimeout()).thenReturn(Duration.ofSeconds(30));
        when(connectorConfig.hostname()).thenReturn("localhost");
        when(connectorConfig.port()).thenReturn(3306);
        when(connectorConfig.username()).thenReturn("test_user");
        when(connectorConfig.sslMode())
                .thenReturn(MySqlConnectorConfig.SecureConnectionMode.DISABLED);

        // Mock getConfig() method that's called in constructor
        Configuration defaultConfig = createConfiguration(false, 3, 60000L);
        when(connectorConfig.getConfig()).thenReturn(defaultConfig);

        when(taskContext.getBinaryLogClient()).thenReturn(binaryLogClient);
        when(taskContext.getSchema()).thenReturn(schema);
        when(connectorConfig.getSkippedOperations()).thenReturn(EnumSet.noneOf(Operation.class));

        // Mock offsetContext.getSource() that's called in execute method
        when(offsetContext.getSource()).thenReturn(sourceInfo);
        when(sourceInfo.binlogFilename()).thenReturn("mysql-bin.000001");

        streamingSource =
                new MySqlStreamingChangeEventSource(
                        connectorConfig,
                        connection,
                        eventDispatcher,
                        errorHandler,
                        testClock,
                        taskContext,
                        metrics);
    }

    @Test
    void testFailOnReconnectionError_TrueWithMaxRetriesReached() throws Exception {
        // Setup configuration
        Configuration config = createConfiguration(true, 3, 60000L); // 1 minute timeout
        when(connectorConfig.getConfig()).thenReturn(config);

        // Mock client behavior: initially connected, then disconnected, then failed reconnections
        AtomicInteger connectCallCount = new AtomicInteger(0);
        when(binaryLogClient.isConnected())
                .thenReturn(true) // Initial connection check passes
                .thenReturn(false) // Then it becomes disconnected
                .thenReturn(false) // Stays disconnected during reconnection attempts
                .thenReturn(false)
                .thenReturn(false);

        // Let first connection succeed, then fail subsequent reconnection attempts
        doAnswer(
                        invocation -> {
                            int callCount = connectCallCount.incrementAndGet();
                            if (callCount == 1) {
                                return null; // First connection succeeds
                            } else {
                                throw new SocketTimeoutException("Connection timeout");
                            }
                        })
                .when(binaryLogClient)
                .connect(anyLong());

        // Create test context
        TestChangeEventSourceContext context = new TestChangeEventSourceContext();
        context.setMaxIterations(10); // Stop after reasonable iterations
        MySqlPartition partition = new MySqlPartition("test-server");
        MySqlOffsetContext testOffsetContext = this.offsetContext;

        // Execute and expect failure after max retries
        assertThatThrownBy(() -> streamingSource.execute(context, partition, testOffsetContext))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Failed to reconnect to MySQL binlog after");

        // Verify reconnection attempts were made (1 initial + 3 reconnection attempts)
        verify(binaryLogClient, times(4)).connect(anyLong());
    }

    @Test
    void testFailOnReconnectionError_TrueWithTimeoutExceeded() throws Exception {
        // Setup configuration with short timeout
        Configuration config = createConfiguration(true, 10, 1000L); // 1 second timeout
        when(connectorConfig.getConfig()).thenReturn(config);

        // Mock client behavior: initially connected, then disconnected, then failed reconnections
        AtomicInteger connectCallCount = new AtomicInteger(0);
        when(binaryLogClient.isConnected())
                .thenReturn(true) // Initial connection check passes
                .thenReturn(false) // Then it becomes disconnected
                .thenReturn(false) // Stays disconnected during reconnection attempts
                .thenReturn(false)
                .thenReturn(false);

        // Let first connection succeed, then fail subsequent reconnection attempts
        doAnswer(
                        invocation -> {
                            int callCount = connectCallCount.incrementAndGet();
                            if (callCount == 1) {
                                return null; // First connection succeeds
                            } else {
                                // Advance clock on each reconnection attempt to simulate timeout
                                testClock.advanceTime(500); // Each attempt takes 500ms
                                throw new SocketTimeoutException("Connection timeout");
                            }
                        })
                .when(binaryLogClient)
                .connect(anyLong());

        TestChangeEventSourceContext context = new TestChangeEventSourceContext();
        context.setMaxIterations(10); // Stop after reasonable iterations
        MySqlPartition partition = new MySqlPartition("test-server");
        MySqlOffsetContext testOffsetContext = this.offsetContext;

        assertThatThrownBy(() -> streamingSource.execute(context, partition, testOffsetContext))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Failed to reconnect to MySQL binlog after");
    }

    @Test
    void testAuthenticationException_FailsWhenFailOnErrorTrue() throws Exception {
        Configuration config = createConfiguration(true, 3, 60000L);
        when(connectorConfig.getConfig()).thenReturn(config);

        // Mock client behavior: initially connected, then disconnected, then auth failures during
        // reconnection
        AtomicInteger connectCallCount = new AtomicInteger(0);
        when(binaryLogClient.isConnected())
                .thenReturn(true) // Initial connection check passes
                .thenReturn(false) // Then it becomes disconnected
                .thenReturn(false) // Stays disconnected during reconnection attempts
                .thenReturn(false)
                .thenReturn(false);

        // Let first connection succeed, then fail subsequent reconnection attempts with
        // AuthenticationException
        doAnswer(
                        invocation -> {
                            int callCount = connectCallCount.incrementAndGet();
                            if (callCount == 1) {
                                return null; // First connection succeeds
                            } else {
                                throw new AuthenticationException("Invalid credentials");
                            }
                        })
                .when(binaryLogClient)
                .connect(anyLong());

        TestChangeEventSourceContext context = new TestChangeEventSourceContext();
        context.setMaxIterations(10); // Stop after reasonable iterations
        MySqlPartition partition = new MySqlPartition("test-server");
        MySqlOffsetContext testOffsetContext = this.offsetContext;

        assertThatThrownBy(() -> streamingSource.execute(context, partition, testOffsetContext))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Authentication failure detected during reconnection")
                .hasCauseInstanceOf(AuthenticationException.class);

        // Verify reconnection attempts were made (1 initial + 3 reconnection attempts)
        verify(binaryLogClient, times(4)).connect(anyLong());
    }

    @Test
    void testAuthenticationException_RespectsRetryLimitsWhenFailOnErrorTrue() throws Exception {
        Configuration config = createConfiguration(true, 3, 60000L);
        when(connectorConfig.getConfig()).thenReturn(config);

        // Mock client behavior: initially connected, then disconnected, then auth failures during
        // reconnection
        AtomicInteger connectCallCount = new AtomicInteger(0);
        when(binaryLogClient.isConnected())
                .thenReturn(true) // Initial connection check passes
                .thenReturn(false) // Then it becomes disconnected
                .thenReturn(false) // Stays disconnected during reconnection attempts
                .thenReturn(false)
                .thenReturn(false);

        // Let first connection succeed, then fail subsequent reconnection attempts with
        // AuthenticationException
        doAnswer(
                        invocation -> {
                            int callCount = connectCallCount.incrementAndGet();
                            if (callCount == 1) {
                                return null; // First connection succeeds
                            } else {
                                throw new AuthenticationException("Invalid credentials");
                            }
                        })
                .when(binaryLogClient)
                .connect(anyLong());

        TestChangeEventSourceContext context = new TestChangeEventSourceContext();
        context.setMaxIterations(10); // Stop after reasonable iterations
        MySqlPartition partition = new MySqlPartition("test-server");
        MySqlOffsetContext testOffsetContext = this.offsetContext;

        assertThatThrownBy(() -> streamingSource.execute(context, partition, testOffsetContext))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Authentication failure detected during reconnection");

        // Should respect retry limits even for auth errors (1 initial + 3 reconnection attempts)
        verify(binaryLogClient, times(4)).connect(anyLong());
    }

    @Test
    void testAuthenticationException_RetriesInfinitelyWhenFailOnErrorFalse() throws Exception {
        Configuration config = createConfiguration(false, 3, 60000L);
        when(connectorConfig.getConfig()).thenReturn(config);

        // Mock client behavior: initially connected, then disconnected, then auth failures during
        // reconnection
        AtomicInteger connectCallCount = new AtomicInteger(0);
        when(binaryLogClient.isConnected())
                .thenReturn(true) // Initial connection check passes
                .thenReturn(false) // Then it becomes disconnected
                .thenReturn(false) // Stays disconnected during first 4 reconnection attempts
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true); // Finally becomes connected after 5th reconnection attempt

        // Let first connection succeed, then fail subsequent reconnection attempts with
        // AuthenticationException
        doAnswer(
                        invocation -> {
                            int callCount = connectCallCount.incrementAndGet();
                            if (callCount == 1) {
                                return null; // First connection succeeds
                            } else if (callCount <= 5) {
                                throw new AuthenticationException("Invalid credentials");
                            } else {
                                return null; // Eventually succeed to stop the test
                            }
                        })
                .when(binaryLogClient)
                .connect(anyLong());

        TestChangeEventSourceContext context = new TestChangeEventSourceContext();
        context.setMaxIterations(10); // Stop after reasonable iterations
        MySqlPartition partition = new MySqlPartition("test-server");
        MySqlOffsetContext testOffsetContext = this.offsetContext;

        // Should keep retrying even with auth errors when failOnError=false (no exception thrown)
        streamingSource.execute(context, partition, testOffsetContext);

        // Should have attempted multiple reconnections (1 initial + 5 reconnection attempts)
        verify(binaryLogClient, times(6)).connect(anyLong());
    }

    @Test
    void testSuccessfulReconnection_ResetsRetryTracking() throws Exception {
        Configuration config = createConfiguration(true, 3, 60000L);
        when(connectorConfig.getConfig()).thenReturn(config);

        // Mock client behavior: initially connected, then disconnected, then reconnect successfully
        AtomicInteger connectCallCount = new AtomicInteger(0);
        when(binaryLogClient.isConnected())
                .thenReturn(true) // Initial connection check passes
                .thenReturn(false) // Then it becomes disconnected
                .thenReturn(false) // First reconnection attempt fails (still disconnected)
                .thenReturn(false) // Second reconnection attempt fails (still disconnected)
                .thenReturn(true); // Third reconnection attempt succeeds (connected)

        // Let first connection succeed, then fail 2 reconnection attempts, then succeed
        doAnswer(
                        invocation -> {
                            int callCount = connectCallCount.incrementAndGet();
                            if (callCount == 1) {
                                return null; // First connection succeeds
                            } else if (callCount == 2) {
                                throw new SocketTimeoutException("Connection timeout");
                            } else if (callCount == 3) {
                                throw new SocketTimeoutException("Connection timeout");
                            } else {
                                return null; // Fourth attempt (3rd reconnection) succeeds
                            }
                        })
                .when(binaryLogClient)
                .connect(anyLong());

        TestChangeEventSourceContext context = new TestChangeEventSourceContext();
        context.setMaxIterations(6); // Enough to see successful reconnection
        MySqlPartition partition = new MySqlPartition("test-server");
        MySqlOffsetContext testOffsetContext = this.offsetContext;

        // Should not throw exception - connection succeeds after reconnection attempts
        streamingSource.execute(context, partition, testOffsetContext);

        // Verify: 1 initial + 3 reconnection attempts (2 failed, 1 succeeded)
        verify(binaryLogClient, times(4)).connect(anyLong());
    }

    private Configuration createConfiguration(boolean failOnError, int maxRetries, long timeoutMs) {
        Properties props = new Properties();
        props.setProperty(
                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(),
                String.valueOf(failOnError));
        props.setProperty(
                MySqlSourceOptions.BINLOG_RECONNECTION_MAX_RETRIES.key(),
                String.valueOf(maxRetries));
        props.setProperty(
                MySqlSourceOptions.BINLOG_RECONNECTION_TIMEOUT.key(), String.valueOf(timeoutMs));
        return Configuration.from(props);
    }

    /** Test implementation of ChangeEventSourceContext for controlling test execution. */
    private static class TestChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicInteger iterations = new AtomicInteger(0);
        private int maxIterations = Integer.MAX_VALUE;

        @Override
        public boolean isRunning() {
            int currentIteration = iterations.incrementAndGet();
            if (currentIteration >= maxIterations) {
                running.set(false);
            }
            return running.get();
        }

        public void setMaxIterations(int maxIterations) {
            this.maxIterations = maxIterations;
        }

        public void stop() {
            running.set(false);
        }
    }

    /** Test clock implementation for controlling time in tests. */
    private static class TestClock implements Clock {
        private long currentTimeMillis = 1000000; // Start at some arbitrary time

        @Override
        public long currentTimeInMillis() {
            return currentTimeMillis;
        }

        public void advanceTime(long milliseconds) {
            currentTimeMillis += milliseconds;
        }
    }
}
