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

package com.ververica.cdc.connectors.oceanbase.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.LogMessage;
import com.ververica.cdc.connectors.oceanbase.source.deserializer.OceanBaseChangeEventDeserializerSchema;
import com.ververica.cdc.connectors.oceanbase.source.deserializer.OceanBaseSnapshotEventDeserializerSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The source implementation for OceanBase that read snapshot events first and then read the change
 * event.
 *
 * @param <T> The type created by the deserializer.
 */
public class OceanBaseRichParallelSourceFunction<T> extends RichSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseRichParallelSourceFunction.class);

    private static final long serialVersionUID = 1L;

    private final TypeInformation<T> resultTypeInfo;
    private final boolean snapshot;
    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;
    private final String jdbcUrl;
    private final String jdbcDriver;
    private final String rsList;
    private final String logProxyHost;
    private final int logProxyPort;
    private final long startTimestamp;
    private final OceanBaseSnapshotEventDeserializerSchema<T> snapshotEventDeserializer;
    private final OceanBaseChangeEventDeserializerSchema<T> changeEventDeserializer;

    private final AtomicBoolean snapshotCompleted = new AtomicBoolean(false);
    private final List<T> transactionBuffer = new LinkedList<>();

    private transient volatile long resolvedTimestamp;
    private transient Connection snapshotConnection;
    private transient LogProxyClient logProxyClient;
    private transient ListState<Long> offsetState;
    private transient OutputCollector<T> outputCollector;

    public OceanBaseRichParallelSourceFunction(
            TypeInformation<T> resultTypeInfo,
            boolean snapshot,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String jdbcUrl,
            String jdbcDriver,
            String rsList,
            String logProxyHost,
            int logProxyPort,
            long startTimestamp,
            OceanBaseSnapshotEventDeserializerSchema<T> snapshotEventDeserializer,
            OceanBaseChangeEventDeserializerSchema<T> changeEventDeserializer) {
        this.resultTypeInfo = resultTypeInfo;
        this.snapshot = snapshot;
        this.username = username;
        this.password = password;
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.jdbcUrl = jdbcUrl;
        this.jdbcDriver = jdbcDriver;
        this.rsList = rsList;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.startTimestamp = startTimestamp;
        this.snapshotEventDeserializer = snapshotEventDeserializer;
        this.changeEventDeserializer = changeEventDeserializer;
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        this.outputCollector = new OutputCollector<>();
        this.resolvedTimestamp = -1;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        outputCollector.context = ctx;

        LOG.info("Start readChangeEvents process");
        readChangeEvents();

        if (shouldReadSnapshot()) {
            synchronized (ctx.getCheckpointLock()) {
                readSnapshotEvents();
                LOG.info("Snapshot reading finished");
            }
        } else {
            LOG.info("Skip snapshot read");
        }

        logProxyClient.join();
    }

    private boolean shouldReadSnapshot() {
        return resolvedTimestamp == -1 && snapshot;
    }

    protected void readSnapshotEvents() throws Exception {
        if (snapshotEventDeserializer == null) {
            LOG.warn("SnapshotEventDeserializer not set");
            return;
        }

        String tableFullName = String.format("`%s`.`%s`", databaseName, tableName);
        String selectTableSql = "SELECT * FROM " + tableFullName;
        Statement stmt = null;
        try {
            Class.forName(jdbcDriver);
            snapshotConnection = DriverManager.getConnection(jdbcUrl, username, password);
            stmt = snapshotConnection.createStatement();
            ResultSet rs = stmt.executeQuery(selectTableSql);
            while (rs.next()) {
                T data = snapshotEventDeserializer.deserialize(rs);
                outputCollector.collect(data);
            }
        } catch (SQLException e) {
            LOG.error("Failed to read snapshot from table: {}", tableFullName, e);
            throw e;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (snapshotConnection != null) {
                snapshotConnection.close();
            }
        }

        snapshotCompleted.set(true);
    }

    protected void readChangeEvents() throws InterruptedException {
        if (changeEventDeserializer == null) {
            LOG.warn("ChangeEventDeserializer not set");
            return;
        }

        ObReaderConfig obReaderConfig = new ObReaderConfig();
        obReaderConfig.setRsList(rsList);
        obReaderConfig.setUsername(username);
        obReaderConfig.setPassword(password);
        obReaderConfig.setTableWhiteList(
                String.format("%s.%s.%s", tenantName, databaseName, tableName));

        if (resolvedTimestamp > 0) {
            obReaderConfig.setStartTimestamp(resolvedTimestamp);
            LOG.info("Read change events from resolvedTimestamp: {}", resolvedTimestamp);
        } else {
            obReaderConfig.setStartTimestamp(startTimestamp);
            LOG.info("Read change events from startTimestamp: {}", startTimestamp);
        }

        final CountDownLatch latch = new CountDownLatch(1);

        logProxyClient = new LogProxyClient(logProxyHost, logProxyPort, obReaderConfig);

        logProxyClient.addListener(
                new RecordListener() {

                    boolean started = false;

                    @Override
                    public void notify(LogMessage message) {
                        switch (message.getOpt()) {
                            case HEARTBEAT:
                            case BEGIN:
                                if (!started) {
                                    started = true;
                                    latch.countDown();
                                }
                                break;
                            case INSERT:
                            case UPDATE:
                            case DELETE:
                                if (!started) {
                                    break;
                                }
                                transactionBuffer.addAll(
                                        changeEventDeserializer.deserialize(message));
                                break;
                            case COMMIT:
                                // flush buffer after snapshot completed
                                if (!shouldReadSnapshot() || snapshotCompleted.get()) {
                                    transactionBuffer.forEach(outputCollector::collect);
                                    transactionBuffer.clear();
                                    resolvedTimestamp = Long.parseLong(message.getTimestamp());
                                }
                                break;
                            case DDL:
                                LOG.trace(
                                        "Ddl: {}",
                                        message.getFieldList().get(0).getValue().toString());
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported type: " + message.getOpt());
                        }
                    }

                    @Override
                    public void onException(LogProxyClientException e) {
                        LOG.error("LogProxyClient exception", e);
                        logProxyClient.stop();
                    }
                });

        logProxyClient.start();
        LOG.info("LogProxyClient started");
        latch.await();
        LOG.info("LogProxyClient packet processing started");
    }

    @Override
    public void notifyCheckpointComplete(long l) {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.resultTypeInfo;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info(
                "snapshotState checkpoint: {} at resolvedTimestamp: {}",
                context.getCheckpointId(),
                resolvedTimestamp);
        offsetState.clear();
        offsetState.add(resolvedTimestamp);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint");
        offsetState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "resolvedTimestampState", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            for (final Long offset : offsetState.get()) {
                resolvedTimestamp = offset;
                LOG.info("Restore State from resolvedTimestamp: {}", resolvedTimestamp);
                return;
            }
        }
    }

    @Override
    public void cancel() {
        try {
            if (snapshotConnection != null) {
                snapshotConnection.close();
            }
        } catch (SQLException e) {
            LOG.error("Failed to close snapshotConnection", e);
        }
        if (logProxyClient != null) {
            logProxyClient.stop();
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
