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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.pipeline.DecimalPrecisionMode;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An async post-transform function for ordered async execution.
 *
 * <p>{@link SchemaChangeEvent}s are handled as barriers. The function waits for all pending {@link
 * DataChangeEvent} futures submitted before the schema change, applies the schema change, and only
 * then allows following data changes to run against the updated schema.
 */
public class AsyncPostTransformFunction extends RichAsyncFunction<Event, Event>
        implements CheckpointedFunction, Serializable {

    private static final long serialVersionUID = 1L;
    private static final String TABLE_STATE_NAME = "async-post-transform-table-state";
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 30L;
    private static final Logger LOG = LoggerFactory.getLogger(AsyncPostTransformFunction.class);

    private final PostTransformProcessor processor;
    private final int asyncWorkerThreads;

    private transient ExecutorService executorService;
    private transient Set<CompletableFuture<List<Event>>> pendingDataFutures;
    private transient CompletableFuture<Void> schemaBarrierFuture;
    private transient ListState<byte[]> tableState;
    private transient Set<TableId> emittedCreateTableEventTables;

    public static AsyncPostTransformFunctionBuilder newBuilder() {
        return new AsyncPostTransformFunctionBuilder();
    }

    AsyncPostTransformFunction(
            List<TransformRule> transformRules,
            String timezone,
            DecimalPrecisionMode decimalPrecisionMode,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions,
            Map<String, AiModelClient> modelClients,
            int asyncWorkerThreads) {
        Preconditions.checkArgument(
                asyncWorkerThreads > 0, "Async worker threads must be greater than 0.");
        this.processor =
                new PostTransformProcessor(
                        transformRules, timezone, decimalPrecisionMode, udfFunctions, modelClients);
        this.asyncWorkerThreads = asyncWorkerThreads;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.pendingDataFutures = ConcurrentHashMap.newKeySet();
        this.schemaBarrierFuture = CompletableFuture.completedFuture(null);
        this.emittedCreateTableEventTables = ConcurrentHashMap.newKeySet();
        processor.open();
        this.executorService =
                Executors.newFixedThreadPool(
                        asyncWorkerThreads,
                        new ThreadFactoryBuilder()
                                .setNameFormat(
                                        "post-transform-async-"
                                                + getRuntimeContext()
                                                        .getTaskInfo()
                                                        .getIndexOfThisSubtask()
                                                + "-%d")
                                .build());
    }

    @Override
    public void close() throws Exception {
        try {
            boolean executorTerminated = true;
            if (executorService != null) {
                executorService.shutdownNow();
                executorTerminated =
                        executorService.awaitTermination(
                                EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            if (executorTerminated) {
                processor.close();
            } else {
                LOG.warn(
                        "Async post-transform workers did not terminate within {} seconds; "
                                + "processor resources will remain open to avoid concurrent close.",
                        EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
            }
        } finally {
            super.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        tableState.clear();
        for (byte[] serializedTableState : processor.serializeTableStates()) {
            tableState.add(serializedTableState);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        tableState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>(TABLE_STATE_NAME, byte[].class));
        if (context.isRestored()) {
            for (byte[] serializedTableState : tableState.get()) {
                processor.restoreTableState(serializedTableState);
            }
        }
    }

    @Override
    public void asyncInvoke(Event event, ResultFuture<Event> resultFuture) {
        if (event instanceof CreateTableEvent) {
            // asyncInvoke runs on the mailbox thread. Marking here prevents a following data event
            // from prepending the same CreateTableEvent while this barrier is processed by a
            // worker thread.
            emittedCreateTableEventTables.add(((CreateTableEvent) event).tableId());
        }
        if (event instanceof DataChangeEvent) {
            TableId tableId = ((DataChangeEvent) event).tableId();
            List<Event> prependedEvents = prependCreateTableEventIfNeeded(tableId);
            asyncInvokeDataChangeEvent(event, resultFuture, prependedEvents);
        } else {
            asyncInvokeBarrierEvent(event, resultFuture);
        }
    }

    @Override
    public void timeout(Event event, ResultFuture<Event> resultFuture) {
        resultFuture.completeExceptionally(
                new FlinkRuntimeException("Async post-transform timed out for event: " + event));
    }

    private void asyncInvokeDataChangeEvent(
            Event event, ResultFuture<Event> resultFuture, List<Event> prependedEvents) {
        CompletableFuture<List<Event>> dataFuture =
                schemaBarrierFuture.thenCompose(
                        ignored ->
                                CompletableFuture.supplyAsync(
                                        () -> processSafely(event), executorService));
        pendingDataFutures.add(dataFuture);
        dataFuture.whenComplete(
                (result, error) -> {
                    pendingDataFutures.remove(dataFuture);
                    if (error != null) {
                        completeResultFuture(event, resultFuture, null, error);
                    } else {
                        completeResultFuture(
                                event, resultFuture, prependEvents(prependedEvents, result), null);
                    }
                });
    }

    private void asyncInvokeBarrierEvent(Event event, ResultFuture<Event> resultFuture) {
        CompletableFuture<Void> previousSchemaBarrierFuture = schemaBarrierFuture;
        CompletableFuture<Void> previousDataFutures = waitForPendingDataFutures();
        CompletableFuture<List<Event>> schemaFuture =
                CompletableFuture.allOf(previousSchemaBarrierFuture, previousDataFutures)
                        .thenApply(ignored -> processSafely(event));
        schemaBarrierFuture = schemaFuture.thenApply(ignored -> null);
        schemaFuture.whenComplete(
                (result, error) -> completeResultFuture(event, resultFuture, result, error));
    }

    private CompletableFuture<Void> waitForPendingDataFutures() {
        CompletableFuture<?>[] futures = pendingDataFutures.toArray(new CompletableFuture<?>[0]);
        return CompletableFuture.allOf(futures);
    }

    private List<Event> processSafely(Event event) {
        try {
            Optional<Event> result = processor.process(event);
            return result.map(Collections::singletonList).orElseGet(Collections::emptyList);
        } catch (Exception e) {
            throw processor.wrapTransformException("async post-transform", event, e);
        }
    }

    private void completeResultFuture(
            Event event,
            ResultFuture<Event> resultFuture,
            @Nullable List<Event> result,
            @Nullable Throwable error) {
        if (error != null) {
            resultFuture.completeExceptionally(
                    processor.wrapTransformException("async post-transform", event, error));
        } else {
            resultFuture.complete(result);
        }
    }

    private List<Event> prependCreateTableEventIfNeeded(TableId tableId) {
        if (!emittedCreateTableEventTables.add(tableId)) {
            return Collections.emptyList();
        }

        CreateTableEvent outputCreateTableEvent = processor.getOutputCreateTableEvent(tableId);
        if (outputCreateTableEvent == null) {
            emittedCreateTableEventTables.remove(tableId);
            return Collections.emptyList();
        }
        return Collections.singletonList(outputCreateTableEvent);
    }

    private static List<Event> prependEvents(
            List<Event> prependedEvents, @Nullable List<Event> result) {
        if (prependedEvents.isEmpty()) {
            return result;
        }
        List<Event> output = new ArrayList<>(prependedEvents);
        if (result != null) {
            output.addAll(result);
        }
        return output;
    }
}
