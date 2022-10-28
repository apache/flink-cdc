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

package com.ververica.cdc.connectors.tdsql.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.MySqlValidator;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReaderContext;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplitSerializer;
import com.ververica.cdc.connectors.tdsql.source.assigner.state.TdSqlPendingSplitsState;
import com.ververica.cdc.connectors.tdsql.source.assigner.state.TdSqlPendingSplitsStateSerializer;
import com.ververica.cdc.connectors.tdsql.source.enumerator.TdSqlSourceEnumerator;
import com.ververica.cdc.connectors.tdsql.source.reader.TdSqlRecordEmitter;
import com.ververica.cdc.connectors.tdsql.source.reader.TdSqlSourceReader;
import com.ververica.cdc.connectors.tdsql.source.reader.TdSqlSplitReader;
import com.ververica.cdc.connectors.tdsql.source.reader.fetcher.TdSqlFetcherManager;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.discoverCapturedTables;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.isTableIdCaseSensitive;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.openJdbcConnection;
import static com.ververica.cdc.connectors.tdsql.bases.TdSqlUtils.discoverSets;

/**
 * The TdSql CDC Source based on {@link com.ververica.cdc.connectors.mysql.source.MySqlSource}.
 * TdSql CDC Source usage as same as MySql CDC Source.
 *
 * @param <T> the output type of the source.
 */
@Internal
public class TdSqlSource<T>
        implements Source<T, TdSqlSplit, TdSqlPendingSplitsState>, ResultTypeQueryable<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TdSqlSource.class);
    private static final long serialVersionUID = 943718471756513230L;

    private final MySqlSourceConfigFactory configFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;

    private List<TdSqlSet> testSets;

    TdSqlSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
    }

    @PublicEvolving
    public static <T> TdSqlSourceBuilder<T> builder() {
        return new TdSqlSourceBuilder<>();
    }

    @VisibleForTesting
    public void setTestSets(List<TdSqlSet> sets) {
        LOGGER.info("set tdsql sets: {}", JSON.toString(sets));
        this.testSets = sets;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, TdSqlSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        MySqlSourceConfig sourceConfig =
                configFactory.createConfig(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);

        final MySqlSourceReaderMetrics sourceReaderMetrics =
                new MySqlSourceReaderMetrics(metricGroup);
        sourceReaderMetrics.registerMetrics();
        MySqlSourceReaderContext mySqlSourceReaderContext =
                new MySqlSourceReaderContext(readerContext);
        Supplier<TdSqlSplitReader> splitReaderSupplier =
                () ->
                        new TdSqlSplitReader(
                                set ->
                                        addMySqlSplitReaderBySet(
                                                set,
                                                readerContext.getIndexOfSubtask(),
                                                mySqlSourceReaderContext));

        return new TdSqlSourceReader<>(
                elementsQueue,
                new TdSqlFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                new TdSqlRecordEmitter<>(
                        deserializationSchema,
                        sourceReaderMetrics,
                        sourceConfig.isIncludeSchemaChanges()),
                readerContext.getConfiguration(),
                mySqlSourceReaderContext.getSourceReaderContext(),
                set -> createConfig(set.getHost(), set.getPort()));
    }

    private MySqlSplitReader addMySqlSplitReaderBySet(
            TdSqlSet set, int subtaskId, MySqlSourceReaderContext context) {
        configFactory.port(set.getPort());
        configFactory.hostname(set.getHost());
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
        return new MySqlSplitReader(sourceConfig, subtaskId, context);
    }

    @Override
    public SplitEnumerator<TdSqlSplit, TdSqlPendingSplitsState> createEnumerator(
            SplitEnumeratorContext<TdSqlSplit> enumContext) throws Exception {
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
        new MySqlValidator(sourceConfig).validate();

        List<TdSqlSet> sets;
        Map<TdSqlSet, MySqlSplitAssigner> assignerMap = new HashMap<>();

        if (testSets != null && testSets.size() > 0) {
            sets = this.testSets;
        } else {
            try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
                sets = discoverSets(jdbc);
            }
        }
        boolean initialStartMode =
                sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL;

        for (TdSqlSet set : sets) {
            LOGGER.trace("init tdsql set {} assigner.", set.getSetKey());
            MySqlSplitAssigner splitAssigner;

            MySqlSourceConfig setSourceConfig = createConfig(set.getHost(), set.getPort());

            if (initialStartMode) {
                try (JdbcConnection setJdbc = openJdbcConnection(setSourceConfig)) {
                    final List<TableId> remainingTables =
                            discoverCapturedTables(setJdbc, setSourceConfig);
                    boolean isTableIdCaseSensitive = isTableIdCaseSensitive(setJdbc);
                    splitAssigner =
                            new MySqlHybridSplitAssigner(
                                    setSourceConfig,
                                    enumContext.currentParallelism(),
                                    remainingTables,
                                    isTableIdCaseSensitive);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Failed to discover captured tables for enumerator", e);
                }
            } else {
                splitAssigner = new MySqlBinlogSplitAssigner(setSourceConfig);
            }

            assignerMap.put(set, splitAssigner);
        }

        return new TdSqlSourceEnumerator(
                enumContext, set -> createConfig(set.getHost(), set.getPort()), assignerMap);
    }

    @Override
    public SplitEnumerator<TdSqlSplit, TdSqlPendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<TdSqlSplit> enumContext, TdSqlPendingSplitsState checkpoint)
            throws Exception {
        Map<TdSqlSet, PendingSplitsState> pendingSplitsStateMap = checkpoint.getStateMap();

        Map<TdSqlSet, MySqlSplitAssigner> assignerMap = new HashMap<>();
        for (TdSqlSet set : pendingSplitsStateMap.keySet()) {
            PendingSplitsState pendingSplitsState = pendingSplitsStateMap.get(set);

            MySqlSourceConfig sourceConfig = createConfig(set.getHost(), set.getPort());

            MySqlSplitAssigner splitAssigner;
            if (pendingSplitsState instanceof HybridPendingSplitsState) {
                splitAssigner =
                        new MySqlHybridSplitAssigner(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                (HybridPendingSplitsState) pendingSplitsState);
            } else if (pendingSplitsState instanceof BinlogPendingSplitsState) {
                splitAssigner =
                        new MySqlBinlogSplitAssigner(
                                sourceConfig, (BinlogPendingSplitsState) pendingSplitsState);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported restored PendingSplitsState: " + checkpoint);
            }

            assignerMap.put(set, splitAssigner);
        }

        return new TdSqlSourceEnumerator(
                enumContext, set -> createConfig(set.getHost(), set.getPort()), assignerMap);
    }

    private MySqlSourceConfig createConfig(String host, int port) {
        configFactory.hostname(host);
        configFactory.port(port);
        return configFactory.createConfig(0);
    }

    @Override
    public SimpleVersionedSerializer<TdSqlSplit> getSplitSerializer() {
        return TdSqlSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<TdSqlPendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new TdSqlPendingSplitsStateSerializer(TdSqlSplitSerializer.INSTANCE);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
