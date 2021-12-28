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

import com.ververica.cdc.connectors.base.schema.BaseSchema;
import com.ververica.cdc.connectors.base.source.assigners.HybridSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigners.SplitAssigner;
import com.ververica.cdc.connectors.base.source.assigners.StreamSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigners.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.base.source.assigners.state.StreamPendingSplitsState;
import com.ververica.cdc.connectors.base.source.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.config.SourceConfigFactory;
import com.ververica.cdc.connectors.base.source.config.StartupMode;
import com.ververica.cdc.connectors.base.source.dialect.SnapshotEventDialect;
import com.ververica.cdc.connectors.base.source.dialect.StreamingEventDialect;
import com.ververica.cdc.connectors.base.source.enumerator.SourceEnumerator;
import com.ververica.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import com.ververica.cdc.connectors.base.source.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.reader.BaseRecordEmitter;
import com.ververica.cdc.connectors.base.source.reader.BaseSplitReader;
import com.ververica.cdc.connectors.base.source.reader.ParallelSourceReader;
import com.ververica.cdc.connectors.base.source.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.split.SourceSplitSerializer;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.Validator;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.function.Supplier;

/**
 * The common CDC Source based on FLIP-27 and Watermark Signal Algorithm which supports parallel
 * reading snapshot of table and then continue to capture data change by streaming reading.
 */
public class ChangeEventHybridSource<T>
        implements Source<T, SourceSplitBase, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final SourceConfigFactory configFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;
    private final OffsetFactory offsetFactory;
    private final SourceSplitSerializer sourceSplitSerializer;
    private final Validator validator;
    private final SnapshotEventDialect snapshotEventDialect;
    private final StreamingEventDialect streamingEventDialect;
    private final BaseSchema baseSchema;

    public ChangeEventHybridSource(
            SourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            SnapshotEventDialect snapshotEventDialect,
            StreamingEventDialect streamingEventDialect,
            BaseSchema baseSchema) {
        this(
                configFactory,
                deserializationSchema,
                offsetFactory,
                snapshotEventDialect,
                streamingEventDialect,
                Validator.getDefaultValidator(),
                baseSchema);
    }

    public ChangeEventHybridSource(
            SourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            SnapshotEventDialect snapshotEventDialect,
            StreamingEventDialect streamingEventDialect,
            Validator validator,
            BaseSchema baseSchema) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
        this.offsetFactory = offsetFactory;
        this.snapshotEventDialect = snapshotEventDialect;
        this.streamingEventDialect = streamingEventDialect;
        this.sourceSplitSerializer =
                new SourceSplitSerializer() {
                    @Override
                    public OffsetFactory getOffsetFactory() {
                        return offsetFactory;
                    }
                };
        this.validator = validator;
        this.baseSchema = baseSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, SourceSplitBase> createReader(SourceReaderContext readerContext) {
        // create source config for the given subtask (e.g. unique server id)
        SourceConfig sourceConfig = configFactory.createConfig(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final SourceReaderMetrics sourceReaderMetrics =
                new SourceReaderMetrics(readerContext.metricGroup());
        sourceReaderMetrics.registerMetrics();
        Supplier<BaseSplitReader> splitReaderSupplier =
                () ->
                        new BaseSplitReader(
                                sourceConfig,
                                readerContext.getIndexOfSubtask(),
                                snapshotEventDialect,
                                streamingEventDialect);
        return new ParallelSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new BaseRecordEmitter<>(
                        deserializationSchema,
                        sourceReaderMetrics,
                        sourceConfig.isIncludeSchemaChanges(),
                        offsetFactory),
                readerContext.getConfiguration(),
                readerContext,
                sourceConfig,
                sourceSplitSerializer,
                snapshotEventDialect);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext) {
        SourceConfig sourceConfig = configFactory.createConfig(0);

        //        validator.validate();

        final SplitAssigner splitAssigner;
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
            try {
                final List<TableId> remainingTables =
                        snapshotEventDialect.discoverCapturedTables(sourceConfig);
                boolean isTableIdCaseSensitive =
                        snapshotEventDialect.isTableIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new HybridSplitAssigner(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive,
                                snapshotEventDialect,
                                offsetFactory,
                                baseSchema);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner =
                    new StreamSplitAssigner(sourceConfig, streamingEventDialect, offsetFactory);
        }

        return new SourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext, PendingSplitsState checkpoint) {
        SourceConfig sourceConfig = configFactory.createConfig(0);

        final SplitAssigner splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new HybridSplitAssigner(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint,
                            snapshotEventDialect,
                            offsetFactory,
                            baseSchema);
        } else if (checkpoint instanceof StreamPendingSplitsState) {
            splitAssigner =
                    new StreamSplitAssigner(
                            sourceConfig,
                            (StreamPendingSplitsState) checkpoint,
                            streamingEventDialect,
                            offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new SourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return sourceSplitSerializer;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        SourceSplitSerializer sourceSplitSerializer = (SourceSplitSerializer) getSplitSerializer();
        return new PendingSplitsStateSerializer(sourceSplitSerializer);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
