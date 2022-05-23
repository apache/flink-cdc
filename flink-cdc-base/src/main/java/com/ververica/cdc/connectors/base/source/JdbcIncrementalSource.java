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

package com.ververica.cdc.connectors.base.source;

import org.apache.flink.annotation.Experimental;
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
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.config.JdbcSourceConfigFactory;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.options.StartupMode;
import com.ververica.cdc.connectors.base.relational.JdbcSourceRecordEmitter;
import com.ververica.cdc.connectors.base.relational.JdbcSourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import com.ververica.cdc.connectors.base.source.reader.JdbcIncrementalSourceReader;
import com.ververica.cdc.connectors.base.source.reader.JdbcSourceSplitReader;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.function.Supplier;

/**
 * The basic source of Incremental Snapshot framework for JDBC datasource, it is based on FLIP-27
 * and Watermark Signal Algorithm which supports parallel reading snapshot of table and then
 * continue to capture data change by streaming reading.
 */
@Experimental
public class JdbcIncrementalSource<T>
        implements Source<
                        T,
                        SourceSplitBase<TableId, TableChange>,
                        PendingSplitsState<TableId, TableChange>>,
                ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final JdbcSourceConfigFactory configFactory;
    private final JdbcDataSourceDialect dataSourceDialect;
    private final OffsetFactory offsetFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;
    private final SourceSplitSerializer<TableId, TableChange> sourceSplitSerializer;

    public JdbcIncrementalSource(
            JdbcSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            JdbcDataSourceDialect dataSourceDialect) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
        this.offsetFactory = offsetFactory;
        this.dataSourceDialect = dataSourceDialect;
        this.sourceSplitSerializer = new JdbcSourceSplitSerializer(offsetFactory);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, SourceSplitBase<TableId, TableChange>> createReader(
            SourceReaderContext readerContext) {
        // create source config for the given subtask (e.g. unique server id)
        JdbcSourceConfig sourceConfig = configFactory.create(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final SourceReaderMetrics sourceReaderMetrics =
                new SourceReaderMetrics(readerContext.metricGroup());
        sourceReaderMetrics.registerMetrics();
        Supplier<JdbcSourceSplitReader> splitReaderSupplier =
                () ->
                        new JdbcSourceSplitReader(
                                readerContext.getIndexOfSubtask(), dataSourceDialect);
        return new JdbcIncrementalSourceReader<T>(
                elementsQueue,
                splitReaderSupplier,
                new JdbcSourceRecordEmitter<>(
                        deserializationSchema,
                        sourceReaderMetrics,
                        sourceConfig.isIncludeSchemaChanges(),
                        offsetFactory),
                readerContext.getConfiguration(),
                readerContext,
                sourceConfig,
                sourceSplitSerializer,
                dataSourceDialect);
    }

    @Override
    public SplitEnumerator<
                    SourceSplitBase<TableId, TableChange>, PendingSplitsState<TableId, TableChange>>
            createEnumerator(
                    SplitEnumeratorContext<SourceSplitBase<TableId, TableChange>> enumContext) {
        JdbcSourceConfig sourceConfig = configFactory.create(0);
        final SplitAssigner<TableId, TableChange, JdbcSourceConfig> splitAssigner;
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
            try {
                final List<TableId> remainingTables =
                        dataSourceDialect.discoverDataCollections(sourceConfig);
                boolean isTableIdCaseSensitive =
                        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new HybridSplitAssigner<>(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive,
                                dataSourceDialect,
                                offsetFactory);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner =
                    new StreamSplitAssigner<>(sourceConfig, dataSourceDialect, offsetFactory);
        }

        return new IncrementalSourceEnumerator<>(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SplitEnumerator<
                    SourceSplitBase<TableId, TableChange>, PendingSplitsState<TableId, TableChange>>
            restoreEnumerator(
                    SplitEnumeratorContext<SourceSplitBase<TableId, TableChange>> enumContext,
                    PendingSplitsState<TableId, TableChange> checkpoint) {
        JdbcSourceConfig sourceConfig = configFactory.create(0);

        final SplitAssigner<TableId, TableChange, JdbcSourceConfig> splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new HybridSplitAssigner<>(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState<TableId, TableChange>) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else if (checkpoint instanceof StreamPendingSplitsState) {
            splitAssigner =
                    new StreamSplitAssigner<>(
                            sourceConfig,
                            (StreamPendingSplitsState<TableId, TableChange>) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new IncrementalSourceEnumerator<>(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase<TableId, TableChange>> getSplitSerializer() {
        return sourceSplitSerializer;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState<TableId, TableChange>>
            getEnumeratorCheckpointSerializer() {
        SourceSplitSerializer<TableId, TableChange> sourceSplitSerializer =
                (SourceSplitSerializer<TableId, TableChange>) getSplitSerializer();
        return new PendingSplitsStateSerializer<>(sourceSplitSerializer);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
