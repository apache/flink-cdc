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

package io.debezium.connector.oracle.logminer;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.unbuffered.UnbufferedLogMinerStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import java.sql.SQLException;

/**
 * Extends {@link UnbufferedLogMinerStreamingChangeEventSource} to expose a {@link
 * #handleRowPreProcessing(LogMinerEventRow)} hook that subclasses can override to intercept
 * LogMiner events before normal processing — used by Flink CDC to detect bounded-split watermarks.
 *
 * <p>Replaces the Debezium 1.9.x vendored copy that exposed a {@code createProcessor()} hook. In
 * Debezium 3.4.2 the processor pattern was removed; the equivalent is {@code processEvent}.
 */
public class LogMinerStreamingChangeEventSource
        extends UnbufferedLogMinerStreamingChangeEventSource {

    public LogMinerStreamingChangeEventSource(
            OracleConnectorConfig connectorConfig,
            OracleConnection jdbcConnection,
            EventDispatcher<OraclePartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            OracleDatabaseSchema schema,
            Configuration jdbcConfig,
            AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics) {
        super(
                connectorConfig,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                jdbcConfig,
                (LogMinerStreamingChangeEventSourceMetrics) streamingMetrics);
    }

    @Override
    protected void processEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        if (!handleRowPreProcessing(row)) {
            super.processEvent(row);
        }
    }

    /**
     * Called before each LogMiner event row is processed. Return {@code true} to skip the row and
     * stop streaming (used for bounded split reads at the high-watermark boundary), or {@code
     * false} to continue with normal event processing.
     */
    protected boolean handleRowPreProcessing(LogMinerEventRow row) throws InterruptedException {
        return false;
    }
}
