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

package com.ververica.cdc.connectors.mongodb.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.options.StartupMode;
import com.ververica.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import com.ververica.cdc.connectors.mongodb.source.enumerator.MongoDBSourceEnumerator;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffsetFactory;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBRecordEmitter;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBSourceReader;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBSourceReaderContext;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBSourceSplitReader;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSourceSplitSerializer;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Supplier;

/**
 * The MongoDB CDC Source based on FLIP-27 which supports parallel reading snapshot of collection
 * and then continue to capture data change from change stream.
 *
 * <pre>
 *     1. The source supports parallel capturing database(s) or collection(s) change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source doesn't need apply any lock of MongoDB.
 * </pre>
 *
 * <pre>{@code
 * MongoDBSource
 *     .<String>builder()
 *     .hosts("localhost:27017")
 *     .databaseList("mydb")
 *     .collectionList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link MongoDBSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 */
@Internal
@Experimental
public class MongoDBSource<T>
        implements Source<
                        T,
                        SourceSplitBase<CollectionId, CollectionSchema>,
                        PendingSplitsState<CollectionId, CollectionSchema>>,
                ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final MongoDBSourceConfigFactory configFactory;
    private final MongoDBDialect dialect;
    private final ChangeStreamOffsetFactory offsetFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;
    private final SourceSplitSerializer<CollectionId, CollectionSchema> sourceSplitSerializer;

    MongoDBSource(
            MongoDBSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            ChangeStreamOffsetFactory offsetFactory,
            MongoDBDialect dialect) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
        this.offsetFactory = offsetFactory;
        this.dialect = dialect;
        this.sourceSplitSerializer = new MongoDBSourceSplitSerializer(offsetFactory);
    }

    /**
     * Get a MongoDBSourceBuilder to build a {@link MongoDBSource}.
     *
     * @return a MongoDB parallel source builder.
     */
    @PublicEvolving
    public static <T> MongoDBSourceBuilder<T> builder() {
        return new MongoDBSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, SourceSplitBase<CollectionId, CollectionSchema>> createReader(
            SourceReaderContext readerContext) throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        MongoDBSourceConfig sourceConfig = configFactory.create(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);

        final SourceReaderMetrics sourceReaderMetrics = new SourceReaderMetrics(metricGroup);
        sourceReaderMetrics.registerMetrics();

        MongoDBSourceReaderContext mongoDBSourceReaderContext =
                new MongoDBSourceReaderContext(readerContext);
        Supplier<MongoDBSourceSplitReader> splitReaderSupplier =
                () ->
                        new MongoDBSourceSplitReader(
                                dialect,
                                sourceConfig,
                                readerContext.getIndexOfSubtask(),
                                mongoDBSourceReaderContext);
        return new MongoDBSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new MongoDBRecordEmitter<>(deserializationSchema, sourceReaderMetrics),
                readerContext.getConfiguration(),
                mongoDBSourceReaderContext,
                sourceConfig);
    }

    @Override
    public MongoDBSourceEnumerator createEnumerator(
            SplitEnumeratorContext<SourceSplitBase<CollectionId, CollectionSchema>> enumContext) {
        MongoDBSourceConfig sourceConfig = configFactory.create(0);

        final SplitAssigner<CollectionId, CollectionSchema, MongoDBSourceConfig> splitAssigner;
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
            try {
                final List<CollectionId> remainingCollections =
                        dialect.discoverDataCollections(sourceConfig);
                boolean isCollectionIdCaseSensitive =
                        dialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new HybridSplitAssigner<>(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingCollections,
                                isCollectionIdCaseSensitive,
                                dialect,
                                offsetFactory);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner = new StreamSplitAssigner<>(sourceConfig, dialect, offsetFactory);
        }

        return new MongoDBSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public MongoDBSourceEnumerator restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase<CollectionId, CollectionSchema>> enumContext,
            PendingSplitsState<CollectionId, CollectionSchema> checkpoint) {
        MongoDBSourceConfig sourceConfig = configFactory.create(0);

        final SplitAssigner<CollectionId, CollectionSchema, MongoDBSourceConfig> splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new HybridSplitAssigner<>(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState<CollectionId, CollectionSchema>) checkpoint,
                            dialect,
                            offsetFactory);
        } else if (checkpoint instanceof StreamPendingSplitsState) {
            splitAssigner =
                    new StreamSplitAssigner<>(
                            sourceConfig,
                            (StreamPendingSplitsState<CollectionId, CollectionSchema>) checkpoint,
                            dialect,
                            offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new MongoDBSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase<CollectionId, CollectionSchema>>
            getSplitSerializer() {
        return sourceSplitSerializer;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState<CollectionId, CollectionSchema>>
            getEnumeratorCheckpointSerializer() {
        SourceSplitSerializer<CollectionId, CollectionSchema> sourceSplitSerializer =
                (SourceSplitSerializer<CollectionId, CollectionSchema>) getSplitSerializer();
        return new PendingSplitsStateSerializer<>(sourceSplitSerializer);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
