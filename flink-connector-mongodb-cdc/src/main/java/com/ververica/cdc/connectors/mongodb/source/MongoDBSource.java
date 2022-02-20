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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;

import com.mongodb.client.MongoClient;
import com.ververica.cdc.connectors.mongodb.source.assigners.MongoDBSplitAssigner;
import com.ververica.cdc.connectors.mongodb.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mongodb.source.assigners.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBChangeStreamConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.connectors.mongodb.source.enumerator.MongoDBSourceEnumerator;
import com.ververica.cdc.connectors.mongodb.source.metrics.MongoDBSourceReaderMetrics;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBHybridSplitReader;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBRecordEmitter;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBSourceReader;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBSourceReaderContext;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBSplitReader;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplitSerializer;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionsFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.clientFor;

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
        implements Source<T, MongoDBSplit, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final MongoDBSourceConfigFactory configFactory;
    private final DebeziumDeserializationSchema<T> deserializationSchema;

    MongoDBSource(
            MongoDBSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
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
    public SourceReader<T, MongoDBSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        MongoDBSourceConfig sourceConfig =
                configFactory.createConfig(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);

        final MongoDBSourceReaderMetrics sourceReaderMetrics =
                new MongoDBSourceReaderMetrics(metricGroup);
        sourceReaderMetrics.registerMetrics();

        MongoDBSourceReaderContext mongoDBSourceReaderContext =
                new MongoDBSourceReaderContext(readerContext);
        Supplier<MongoDBSplitReader<MongoDBSplit>> splitReaderSupplier =
                () ->
                        new MongoDBHybridSplitReader(
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
    public SplitEnumerator<MongoDBSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<MongoDBSplit> enumContext) {
        MongoDBSourceConfig sourceConfig = configFactory.createConfig(0);

        MongoClient mongoClient = clientFor(sourceConfig);
        List<String> discoveredDatabases =
                databaseNames(mongoClient, databaseFilter(sourceConfig.getDatabaseList()));
        List<String> discoveredCollections =
                collectionNames(
                        mongoClient,
                        discoveredDatabases,
                        collectionsFilter(sourceConfig.getCollectionList()));

        MongoDBChangeStreamConfig changeStreamConfig =
                configFactory.createChangeStreamConfig(discoveredDatabases, discoveredCollections);

        List<CollectionId> remainingCollections = new ArrayList<>();
        if (sourceConfig.shouldCopyExisting()) {
            remainingCollections.addAll(CollectionId.parse(discoveredCollections));
        }

        final MongoDBSplitAssigner splitAssigner =
                new MongoDBSplitAssigner(
                        sourceConfig,
                        changeStreamConfig,
                        enumContext.currentParallelism(),
                        remainingCollections);

        return new MongoDBSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SplitEnumerator<MongoDBSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<MongoDBSplit> enumContext, PendingSplitsState checkpoint) {
        MongoDBSourceConfig sourceConfig = configFactory.createConfig(0);

        MongoClient mongoClient = clientFor(sourceConfig);
        List<String> discoveredDatabases =
                databaseNames(mongoClient, databaseFilter(sourceConfig.getDatabaseList()));
        List<String> discoveredCollections =
                collectionNames(
                        mongoClient,
                        discoveredDatabases,
                        collectionsFilter(sourceConfig.getCollectionList()));

        MongoDBChangeStreamConfig changeStreamConfig =
                configFactory.createChangeStreamConfig(discoveredDatabases, discoveredCollections);

        List<CollectionId> discoveredCollectionIds = CollectionId.parse(discoveredCollections);
        List<CollectionId> remainingCollections = new ArrayList<>();
        if (sourceConfig.shouldCopyExisting() && sourceConfig.isScanNewlyAddedCollectionEnabled()) {
            discoveredCollectionIds.removeAll(checkpoint.getAlreadyProcessedCollections());
            discoveredCollectionIds.removeAll(checkpoint.getRemainingCollections());
            remainingCollections.addAll(discoveredCollectionIds);
        } else {
            remainingCollections.addAll(checkpoint.getRemainingCollections());
        }

        final MongoDBSplitAssigner splitAssigner =
                new MongoDBSplitAssigner(
                        sourceConfig,
                        changeStreamConfig,
                        enumContext.currentParallelism(),
                        checkpoint.isStreamSplitAssigned(),
                        remainingCollections,
                        checkpoint.getAlreadyProcessedCollections(),
                        checkpoint.getRemainingSnapshotSplits(),
                        checkpoint.getAssignedSnapshotSplits(),
                        checkpoint.getFinishedSnapshotSplits(),
                        checkpoint.getRemainingStreamSplit(),
                        checkpoint.getAssignerStatus());

        return new MongoDBSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<MongoDBSplit> getSplitSerializer() {
        return MongoDBSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsStateSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
