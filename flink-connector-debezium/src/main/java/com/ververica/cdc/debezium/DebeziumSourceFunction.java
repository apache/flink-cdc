/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.debezium;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.debezium.internal.DebeziumChangeConsumer;
import com.ververica.cdc.debezium.internal.DebeziumChangeFetcher;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import com.ververica.cdc.debezium.internal.DebeziumOffsetSerializer;
import com.ververica.cdc.debezium.internal.FlinkDatabaseHistory;
import com.ververica.cdc.debezium.internal.FlinkDatabaseSchemaHistory;
import com.ververica.cdc.debezium.internal.FlinkOffsetBackingStore;
import com.ververica.cdc.debezium.internal.Handover;
import com.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.heartbeat.Heartbeat;
import org.apache.commons.collections.map.LinkedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.ververica.cdc.debezium.utils.DatabaseHistoryUtil.registerHistory;
import static com.ververica.cdc.debezium.utils.DatabaseHistoryUtil.retrieveHistory;

/**
 * The {@link DebeziumSourceFunction} is a streaming data source that pulls captured change data
 * from databases into Flink.
 *
 * <p>There are two workers during the runtime. One worker periodically pulls records from the
 * database and pushes the records into the {@link Handover}. The other worker consumes the records
 * from the {@link Handover} and convert the records to the data in Flink style. The reason why
 * don't use one workers is because debezium has different behaviours in snapshot phase and
 * streaming phase.
 *
 * <p>Here we use the {@link Handover} as the buffer to submit data from the producer to the
 * consumer. Because the two threads don't communicate to each other directly, the error reporting
 * also relies on {@link Handover}. When the engine gets errors, the engine uses the {@link
 * DebeziumEngine.CompletionCallback} to report errors to the {@link Handover} and wakes up the
 * consumer to check the error. However, the source function just closes the engine and wakes up the
 * producer if the error is from the Flink side.
 *
 * <p>If the execution is canceled or finish(only snapshot phase), the exit logic is as same as the
 * logic in the error reporting.
 *
 * <p>The source function participates in checkpointing and guarantees that no data is lost during a
 * failure, and that the computation processes elements "exactly once".
 *
 * <p>Note: currently, the source function can't run in multiple parallel instances.
 *
 * <p>Please refer to Debezium's documentation for the available configuration properties:
 * https://debezium.io/documentation/reference/1.5/development/engine.html#engine-properties
 */
@PublicEvolving
public class DebeziumSourceFunction<T> extends RichSourceFunction<T>
        implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<T> {

    private static final long serialVersionUID = -5808108641062931623L;

    protected static final Logger LOG = LoggerFactory.getLogger(DebeziumSourceFunction.class);

    /** State name of the consumer's partition offset states. */
    public static final String OFFSETS_STATE_NAME = "offset-states";

    /** State name of the consumer's history records state. */
    public static final String HISTORY_RECORDS_STATE_NAME = "history-records-states";

    /** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks. */
    public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

    /**
     * The configuration represents the Debezium MySQL Connector uses the legacy implementation or
     * not.
     */
    public static final String LEGACY_IMPLEMENTATION_KEY = "internal.implementation";

    /** The configuration value represents legacy implementation. */
    public static final String LEGACY_IMPLEMENTATION_VALUE = "legacy";

    // ---------------------------------------------------------------------------------------
    // Properties
    // ---------------------------------------------------------------------------------------

    /** The schema to convert from Debezium's messages into Flink's objects. */
    private final DebeziumDeserializationSchema<T> deserializer;

    /** User-supplied properties for Kafka. * */
    private final Properties properties;

    /** The specific binlog offset to read from when the first startup. */
    private final @Nullable DebeziumOffset specificOffset;

    /** Data for pending but uncommitted offsets. */
    private final LinkedMap pendingOffsetsToCommit = new LinkedMap();

    /** Flag indicating whether the Debezium Engine is started. */
    private volatile boolean debeziumStarted = false;

    /** Validator to validate the connected database satisfies the cdc connector's requirements. */
    private final Validator validator;

    // ---------------------------------------------------------------------------------------
    // State
    // ---------------------------------------------------------------------------------------

    /**
     * The offsets to restore to, if the consumer restores state from a checkpoint.
     *
     * <p>This map will be populated by the {@link #initializeState(FunctionInitializationContext)}
     * method.
     *
     * <p>Using a String because we are encoding the offset state in JSON bytes.
     */
    private transient volatile String restoredOffsetState;

    /** Accessor for state in the operator state backend. */
    private transient ListState<byte[]> offsetState;

    /**
     * State to store the history records, i.e. schema changes.
     *
     * @see FlinkDatabaseHistory
     * @see FlinkDatabaseSchemaHistory
     */
    private transient ListState<String> schemaRecordsState;

    // ---------------------------------------------------------------------------------------
    // Worker
    // ---------------------------------------------------------------------------------------

    private transient ExecutorService executor;
    private transient DebeziumEngine<?> engine;
    /**
     * Unique name of this Debezium Engine instance across all the jobs. Currently we randomly
     * generate a UUID for it. This is used for {@link FlinkDatabaseHistory}.
     */
    private transient String engineInstanceName;

    /** Consume the events from the engine and commit the offset to the engine. */
    private transient DebeziumChangeConsumer changeConsumer;

    /** The consumer to fetch records from {@link Handover}. */
    private transient DebeziumChangeFetcher<T> debeziumChangeFetcher;

    /** Buffer the events from the source and record the errors from the debezium. */
    private transient Handover handover;

    // ---------------------------------------------------------------------------------------

    public DebeziumSourceFunction(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties,
            @Nullable DebeziumOffset specificOffset,
            Validator validator) {
        this.deserializer = deserializer;
        this.properties = properties;
        this.specificOffset = specificOffset;
        this.validator = validator;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        validator.validate();
        super.open(parameters);
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-engine").build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.handover = new Handover();
        this.changeConsumer = new DebeziumChangeConsumer(handover);
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and restore
    // ------------------------------------------------------------------------

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        this.offsetState =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                OFFSETS_STATE_NAME,
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        this.schemaRecordsState =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                HISTORY_RECORDS_STATE_NAME, BasicTypeInfo.STRING_TYPE_INFO));

        if (context.isRestored()) {
            restoreOffsetState();
            restoreHistoryRecordsState();
        } else {
            if (specificOffset != null) {
                byte[] serializedOffset =
                        DebeziumOffsetSerializer.INSTANCE.serialize(specificOffset);
                restoredOffsetState = new String(serializedOffset, StandardCharsets.UTF_8);
                LOG.info(
                        "Consumer subtask {} starts to read from specified offset {}.",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        restoredOffsetState);
            } else {
                LOG.info(
                        "Consumer subtask {} has no restore state.",
                        getRuntimeContext().getIndexOfThisSubtask());
            }
        }
    }

    private void restoreOffsetState() throws Exception {
        for (byte[] serializedOffset : offsetState.get()) {
            if (restoredOffsetState == null) {
                restoredOffsetState = new String(serializedOffset, StandardCharsets.UTF_8);
            } else {
                throw new RuntimeException(
                        "Debezium Source only support single task, "
                                + "however, this is restored from multiple tasks.");
            }
        }
        LOG.info(
                "Consumer subtask {} restored offset state: {}.",
                getRuntimeContext().getIndexOfThisSubtask(),
                restoredOffsetState);
    }

    private void restoreHistoryRecordsState() throws Exception {
        DocumentReader reader = DocumentReader.defaultReader();
        ConcurrentLinkedQueue<SchemaRecord> historyRecords = new ConcurrentLinkedQueue<>();
        int recordsCount = 0;
        boolean firstEntry = true;
        for (String record : schemaRecordsState.get()) {
            if (firstEntry) {
                // we store the engine instance name in the first element
                this.engineInstanceName = record;
                firstEntry = false;
            } else {
                // Put the records into the state. The database history should read, reorganize and
                // register the state.
                historyRecords.add(new SchemaRecord(reader.read(record)));
                recordsCount++;
            }
        }
        if (engineInstanceName != null) {
            registerHistory(engineInstanceName, historyRecords);
        }
        LOG.info(
                "Consumer subtask {} restored history records state: {} with {} records.",
                getRuntimeContext().getIndexOfThisSubtask(),
                engineInstanceName,
                recordsCount);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (handover.hasError()) {
            LOG.debug("snapshotState() called on closed source");
            throw new FlinkRuntimeException(
                    "Call snapshotState() on closed source, checkpoint failed.",
                    handover.getError());
        } else {
            snapshotOffsetState(functionSnapshotContext.getCheckpointId());
            snapshotHistoryRecordsState();
        }
    }

    private void snapshotOffsetState(long checkpointId) throws Exception {
        offsetState.clear();

        final DebeziumChangeFetcher<?> fetcher = this.debeziumChangeFetcher;

        byte[] serializedOffset = null;
        if (fetcher == null) {
            // the fetcher has not yet been initialized, which means we need to return the
            // originally restored offsets
            if (restoredOffsetState != null) {
                serializedOffset = restoredOffsetState.getBytes(StandardCharsets.UTF_8);
            }
        } else {
            byte[] currentState = fetcher.snapshotCurrentState();
            if (currentState == null && restoredOffsetState != null) {
                // the fetcher has been initialized, but has not yet received any data,
                // which means we need to return the originally restored offsets.
                serializedOffset = restoredOffsetState.getBytes(StandardCharsets.UTF_8);
            } else {
                serializedOffset = currentState;
            }
        }

        if (serializedOffset != null) {
            offsetState.add(serializedOffset);
            // the map cannot be asynchronously updated, because only one checkpoint call
            // can happen on this function at a time: either snapshotState() or
            // notifyCheckpointComplete()
            pendingOffsetsToCommit.put(checkpointId, serializedOffset);
            // truncate the map of pending offsets to commit, to prevent infinite growth
            while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
                pendingOffsetsToCommit.remove(0);
            }
        }
    }

    private void snapshotHistoryRecordsState() throws Exception {
        schemaRecordsState.clear();

        if (engineInstanceName != null) {
            schemaRecordsState.add(engineInstanceName);
            Collection<SchemaRecord> records = retrieveHistory(engineInstanceName);
            DocumentWriter writer = DocumentWriter.defaultWriter();
            for (SchemaRecord record : records) {
                schemaRecordsState.add(writer.write(record.toDocument()));
            }
        }
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        properties.setProperty("name", "engine");
        properties.setProperty("offset.storage", FlinkOffsetBackingStore.class.getCanonicalName());
        if (restoredOffsetState != null) {
            // restored from state
            properties.setProperty(FlinkOffsetBackingStore.OFFSET_STATE_VALUE, restoredOffsetState);
        }
        // DO NOT include schema change, e.g. DDL
        properties.setProperty("include.schema.changes", "false");
        // disable the offset flush totally
        properties.setProperty("offset.flush.interval.ms", String.valueOf(Long.MAX_VALUE));
        // disable tombstones
        properties.setProperty("tombstones.on.delete", "false");
        if (engineInstanceName == null) {
            // not restore from recovery
            engineInstanceName = UUID.randomUUID().toString();
        }
        // history instance name to initialize FlinkDatabaseHistory
        properties.setProperty(
                FlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME, engineInstanceName);
        // we have to use a persisted DatabaseHistory implementation, otherwise, recovery can't
        // continue to read binlog
        // see
        // https://stackoverflow.com/questions/57147584/debezium-error-schema-isnt-know-to-this-connector
        // and https://debezium.io/blog/2018/03/16/note-on-database-history-topic-configuration/
        properties.setProperty("database.history", determineDatabase().getCanonicalName());

        // we have to filter out the heartbeat events, otherwise the deserializer will fail
        String dbzHeartbeatPrefix =
                properties.getProperty(
                        Heartbeat.HEARTBEAT_TOPICS_PREFIX.name(),
                        Heartbeat.HEARTBEAT_TOPICS_PREFIX.defaultValueAsString());
        this.debeziumChangeFetcher =
                new DebeziumChangeFetcher<>(
                        sourceContext,
                        deserializer,
                        restoredOffsetState == null, // DB snapshot phase if restore state is null
                        dbzHeartbeatPrefix,
                        handover);

        // create the engine with this configuration ...
        this.engine =
                DebeziumEngine.create(Connect.class)
                        .using(properties)
                        .notifying(changeConsumer)
                        .using(OffsetCommitPolicy.always())
                        .using(
                                (success, message, error) -> {
                                    if (success) {
                                        // Close the handover and prepare to exit.
                                        handover.close();
                                    } else {
                                        handover.reportError(error);
                                    }
                                })
                        .build();

        // run the engine asynchronously
        executor.execute(engine);
        debeziumStarted = true;

        // initialize metrics
        // make RuntimeContext#getMetricGroup compatible between Flink 1.13 and Flink 1.14
        final Method getMetricGroupMethod =
                getRuntimeContext().getClass().getMethod("getMetricGroup");
        getMetricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup =
                (MetricGroup) getMetricGroupMethod.invoke(getRuntimeContext());

        metricGroup.gauge(
                "currentFetchEventTimeLag",
                (Gauge<Long>) () -> debeziumChangeFetcher.getFetchDelay());
        metricGroup.gauge(
                "currentEmitEventTimeLag",
                (Gauge<Long>) () -> debeziumChangeFetcher.getEmitDelay());
        metricGroup.gauge(
                "sourceIdleTime", (Gauge<Long>) () -> debeziumChangeFetcher.getIdleTime());

        // start the real debezium consumer
        debeziumChangeFetcher.runFetchLoop();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (!debeziumStarted) {
            LOG.debug("notifyCheckpointComplete() called when engine is not started.");
            return;
        }

        final DebeziumChangeFetcher<T> fetcher = this.debeziumChangeFetcher;
        if (fetcher == null) {
            LOG.debug("notifyCheckpointComplete() called on uninitialized source");
            return;
        }

        try {
            final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
            if (posInMap == -1) {
                LOG.warn(
                        "Consumer subtask {} received confirmation for unknown checkpoint id {}",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        checkpointId);
                return;
            }

            byte[] serializedOffsets = (byte[]) pendingOffsetsToCommit.remove(posInMap);

            // remove older checkpoints in map
            for (int i = 0; i < posInMap; i++) {
                pendingOffsetsToCommit.remove(0);
            }

            if (serializedOffsets == null || serializedOffsets.length == 0) {
                LOG.debug(
                        "Consumer subtask {} has empty checkpoint state.",
                        getRuntimeContext().getIndexOfThisSubtask());
                return;
            }

            DebeziumOffset offset =
                    DebeziumOffsetSerializer.INSTANCE.deserialize(serializedOffsets);
            changeConsumer.commitOffset(offset);
        } catch (Exception e) {
            // ignore exception if we are no longer running
            LOG.warn("Ignore error when committing offset to database.", e);
        }
    }

    @Override
    public void cancel() {
        // safely and gracefully stop the engine
        shutdownEngine();
        if (debeziumChangeFetcher != null) {
            debeziumChangeFetcher.close();
        }
    }

    @Override
    public void close() throws Exception {
        cancel();

        if (executor != null) {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }

        super.close();
    }

    /** Safely and gracefully stop the Debezium engine. */
    private void shutdownEngine() {
        try {
            if (engine != null) {
                engine.close();
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        } finally {
            if (executor != null) {
                executor.shutdownNow();
            }

            debeziumStarted = false;

            if (handover != null) {
                handover.close();
            }
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    @VisibleForTesting
    public LinkedMap getPendingOffsetsToCommit() {
        return pendingOffsetsToCommit;
    }

    @VisibleForTesting
    public boolean getDebeziumStarted() {
        return debeziumStarted;
    }

    private Class<?> determineDatabase() {
        boolean isCompatibleWithLegacy =
                FlinkDatabaseHistory.isCompatible(retrieveHistory(engineInstanceName));
        if (LEGACY_IMPLEMENTATION_VALUE.equals(properties.get(LEGACY_IMPLEMENTATION_KEY))) {
            // specifies the legacy implementation but the state may be incompatible
            if (isCompatibleWithLegacy) {
                return FlinkDatabaseHistory.class;
            } else {
                throw new IllegalStateException(
                        "The configured option 'debezium.internal.implementation' is 'legacy', but the state of source is incompatible with this implementation, you should remove the the option.");
            }
        } else if (FlinkDatabaseSchemaHistory.isCompatible(retrieveHistory(engineInstanceName))) {
            // tries the non-legacy first
            return FlinkDatabaseSchemaHistory.class;
        } else if (isCompatibleWithLegacy) {
            // fallback to legacy if possible
            return FlinkDatabaseHistory.class;
        } else {
            // impossible
            throw new IllegalStateException("Can't determine which DatabaseHistory to use.");
        }
    }

    @VisibleForTesting
    public String getEngineInstanceName() {
        return engineInstanceName;
    }
}
