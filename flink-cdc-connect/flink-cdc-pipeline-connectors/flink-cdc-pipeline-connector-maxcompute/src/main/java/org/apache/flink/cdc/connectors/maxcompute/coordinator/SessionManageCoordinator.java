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

package org.apache.flink.cdc.connectors.maxcompute.coordinator;

import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.maxcompute.common.Constant;
import org.apache.flink.cdc.connectors.maxcompute.common.FlinkOdpsException;
import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CommitSessionRequest;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CreateSessionRequest;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CreateSessionResponse;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.WaitForFlushSuccessRequest;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;
import org.apache.flink.cdc.connectors.maxcompute.utils.RetryUtils;
import org.apache.flink.cdc.connectors.maxcompute.utils.SessionCommitCoordinateHelper;
import org.apache.flink.cdc.connectors.maxcompute.writer.MaxComputeWriter;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An OperatorCoordinator is used to manage the Session and is consistent with accepting {@link
 * CreateSessionRequest} and {@link CommitSessionRequest} sent by the Operator.
 */
public class SessionManageCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SessionManageCoordinator.class);
    private final String operatorName;
    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final int parallelism;
    private SessionCommitCoordinateHelper sessionCommitCoordinateHelper;
    private Map<SessionIdentifier, MaxComputeWriter> sessionCache;
    private Map<String, SessionIdentifier> sessionIdMap;
    private CompletableFuture<CoordinationResponse>[] waitingFlushFutures;
    private ExecutorService executor;

    private SessionManageCoordinator(
            String operatorName,
            Context context,
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions) {
        this.operatorName = operatorName;
        this.parallelism = context.currentParallelism();
        this.options = options;
        this.writeOptions = writeOptions;
    }

    @Override
    public void start() {
        LOG.info("Starting SessionManageCoordinator {}.", operatorName);

        this.sessionCache = new HashMap<>();
        this.sessionIdMap = new HashMap<>();
        // start the executor
        this.executor = Executors.newFixedThreadPool(writeOptions.getNumCommitThread());

        this.waitingFlushFutures = new CompletableFuture[parallelism];
        this.sessionCommitCoordinateHelper = new SessionCommitCoordinateHelper(parallelism);
    }

    @Override
    public void close() throws Exception {
        if (this.executor != null) {
            this.executor.shutdown();
        }
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        // nothing to do
    }

    private MaxComputeWriter createWriter(SessionIdentifier identifier) {
        String partitionName = identifier.getPartitionName();
        if (!StringUtils.isNullOrWhitespaceOnly(partitionName)) {
            RetryUtils.executeUnchecked(
                    () -> {
                        MaxComputeUtils.createPartitionIfAbsent(
                                options,
                                identifier.getSchema(),
                                identifier.getTable(),
                                partitionName);
                        return null;
                    });
        }
        try {
            MaxComputeWriter writer =
                    MaxComputeWriter.batchWriter(options, writeOptions, identifier);
            LOG.info("Create session for table {}, sessionId {}.", identifier, writer.getId());
            return writer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        executor.execute(
                () -> {
                    try {
                        result.complete(new byte[0]);
                    } catch (Throwable throwable) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(throwable);
                        // when a checkpoint fails, throws directly.
                        result.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "Failed to checkpoint Session %s for source %s",
                                                checkpointId, this.getClass().getSimpleName()),
                                        throwable));
                    }
                });
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        LOG.info("Received coordination request {}.", request);
        if (request instanceof CommitSessionRequest) {
            CommitSessionRequest commitSessionRequest = (CommitSessionRequest) request;

            CompletableFuture<CoordinationResponse> future =
                    sessionCommitCoordinateHelper.commit(
                            commitSessionRequest.getOperatorIndex(),
                            commitSessionRequest.getSessionId());
            String toSubmitSessionId = sessionCommitCoordinateHelper.getToCommitSessionId();
            while (sessionCommitCoordinateHelper.isCommitting() && toSubmitSessionId != null) {
                commitSession(toSubmitSessionId);
                toSubmitSessionId = sessionCommitCoordinateHelper.getToCommitSessionId();
            }
            if (!sessionCommitCoordinateHelper.isCommitting()) {
                sessionCommitCoordinateHelper.commitSuccess(Constant.END_OF_SESSION, true);
                sessionCommitCoordinateHelper.clear();

                if (!sessionCache.isEmpty()) {
                    throw new FlinkOdpsException(
                            "sessionCache not empty: " + sessionCache.keySet());
                }
                completeAllFlushFutures();
            }
            return future;
        } else if (request instanceof WaitForFlushSuccessRequest) {
            CompletableFuture<CoordinationResponse> waitingFlushFuture = new CompletableFuture<>();
            waitingFlushFutures[((WaitForFlushSuccessRequest) request).getOperatorIndex()] =
                    waitingFlushFuture;
            return waitingFlushFuture;
        } else if (request instanceof CreateSessionRequest) {
            SessionIdentifier sessionIdentifier = ((CreateSessionRequest) request).getIdentifier();
            if (!sessionCache.containsKey(sessionIdentifier)) {
                MaxComputeWriter writer = createWriter(sessionIdentifier);
                sessionCache.put(sessionIdentifier, writer);
                sessionIdMap.put(writer.getId(), sessionIdentifier);
            }
            return CompletableFuture.completedFuture(
                    CoordinationResponseUtils.wrap(
                            new CreateSessionResponse(
                                    sessionCache.get(sessionIdentifier).getId())));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private void commitSession(String toSubmitSessionId) {
        MaxComputeWriter writer = sessionCache.remove(sessionIdMap.remove(toSubmitSessionId));
        AtomicBoolean isSuccess = new AtomicBoolean(true);
        LOG.info("start commit writer {}.", toSubmitSessionId);
        try {
            Future<?> future =
                    executor.submit(
                            () -> {
                                try {
                                    writer.commit();
                                } catch (Throwable throwable) {
                                    ExceptionUtils.rethrowIfFatalErrorOrOOM(throwable);
                                    LOG.warn(
                                            "Failed to commit writer {}.",
                                            writer.getId(),
                                            throwable);
                                    isSuccess.set(false);
                                }
                            });
            future.get();
        } catch (Exception e) {
            isSuccess.set(false);
        }
        sessionCommitCoordinateHelper.commitSuccess(toSubmitSessionId, isSuccess.get());
    }

    private void completeAllFlushFutures() {
        for (CompletableFuture<CoordinationResponse> waitingFlushFuture : waitingFlushFutures) {
            waitingFlushFuture.complete(null);
        }
        Arrays.fill(waitingFlushFutures, null);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        // nothing to do
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // nothing to do
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        // nothing to do
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        // nothing to do
    }

    /**
     * The {@link org.apache.flink.runtime.operators.coordination.OperatorCoordinator.Provider} of
     * {@link SessionManageCoordinator}.
     */
    public static class SessionManageCoordinatorProvider implements Provider {

        private final OperatorID operatorID;
        private final String operatorName;
        private final MaxComputeOptions options;
        private final MaxComputeWriteOptions writeOptions;

        public SessionManageCoordinatorProvider(
                String operatorName,
                OperatorID operatorID,
                MaxComputeOptions options,
                MaxComputeWriteOptions writeOptions) {
            this.operatorName = operatorName;
            this.operatorID = operatorID;

            this.options = options;
            this.writeOptions = writeOptions;
        }

        /** Gets the ID of the operator to which the coordinator belongs. */
        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        /**
         * Creates the {@code OperatorCoordinator}, using the given context.
         *
         * @param context
         */
        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            return new SessionManageCoordinator(operatorName, context, options, writeOptions);
        }
    }
}
