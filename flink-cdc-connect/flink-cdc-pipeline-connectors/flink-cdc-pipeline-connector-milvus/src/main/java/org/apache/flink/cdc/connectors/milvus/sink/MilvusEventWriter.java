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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.connectors.milvus.serde.MilvusEventSerializer;
import org.apache.flink.cdc.connectors.milvus.serde.MilvusOperation;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusRetryUtils;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Milvus sink writer. */
public class MilvusEventWriter implements SinkWriter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(MilvusEventWriter.class);

    private final MilvusDataSinkConfig config;
    private final MilvusEventSerializer serializer;
    private final List<MilvusOperation> pendingOperations;
    private final MilvusSinkMetrics metrics;
    private final MilvusClientWrapper client;
    private final Set<String> knownPartitions;
    private long lastFlushTimestamp;

    public MilvusEventWriter(MilvusDataSinkConfig config) throws IOException {
        this(config, MilvusSinkMetrics.NOOP);
    }

    MilvusEventWriter(MilvusDataSinkConfig config, MilvusSinkMetrics metrics) throws IOException {
        this(config, metrics, new DefaultMilvusClientWrapper(config));
    }

    MilvusEventWriter(
            MilvusDataSinkConfig config, MilvusSinkMetrics metrics, MilvusClientWrapper client) {
        this.config = config;
        this.serializer = new MilvusEventSerializer(config);
        this.pendingOperations = new ArrayList<>();
        this.metrics = metrics;
        this.client = client;
        this.knownPartitions = new HashSet<>();
        this.lastFlushTimestamp = System.currentTimeMillis();
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        try {
            if (event instanceof SchemaChangeEvent) {
                flush();
            }
            pendingOperations.addAll(serializer.serialize(event));
            metrics.setPendingRows(pendingOperations.size());
            flushIfNeeded();
        } catch (Exception e) {
            throw new IOException("Failed to write event to Milvus sink: " + event, e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        try {
            flush();
        } catch (Exception e) {
            throw new IOException("Failed to flush Milvus sink.", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            client.close();
        }
    }

    private void flushIfNeeded() {
        long now = System.currentTimeMillis();
        if (pendingOperations.size() >= config.getFlushMaxRows()
                || now - lastFlushTimestamp >= config.getFlushInterval().toMillis()) {
            flush();
        }
    }

    private void flush() {
        if (pendingOperations.isEmpty()) {
            return;
        }
        int rows = pendingOperations.size();
        long startMillis = System.currentTimeMillis();
        RuntimeException failure = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                executePendingOperationsAsOrderedBatches();
                pendingOperations.clear();
                lastFlushTimestamp = System.currentTimeMillis();
                metrics.recordSuccessfulFlush(rows, lastFlushTimestamp - startMillis);
                return;
            } catch (RuntimeException e) {
                failure = e;
                metrics.recordFlushFailure();
                if (!MilvusRetryUtils.isRetryable(e) || attempt >= config.getMaxRetries()) {
                    break;
                }
                metrics.recordRetry();
                LOG.warn(
                        "Failed to flush Milvus batch. Retry {}/{}. ErrorCode={}.",
                        attempt + 1,
                        config.getMaxRetries(),
                        MilvusRetryUtils.classify(e),
                        e);
                reconnectIfNeeded(e);
                sleepBeforeRetry(attempt);
            }
        }
        metrics.recordRecordsOutErrors(pendingOperations.size());
        throw failure;
    }

    private void executePendingOperationsAsOrderedBatches() {
        while (!pendingOperations.isEmpty()) {
            int batchSize = countLeadingBatchSize(pendingOperations);
            MilvusOperation first = pendingOperations.get(0);
            if (first.getType() == MilvusOperation.Type.UPSERT) {
                flushUpsertBatch(first.getCollectionName(), first.getPartitionName(), batchSize);
            } else {
                flushDeleteBatch(first.getCollectionName(), first.getPartitionName(), batchSize);
            }
            pendingOperations.subList(0, batchSize).clear();
            metrics.setPendingRows(pendingOperations.size());
        }
    }

    private void flushUpsertBatch(String collectionName, String partitionName, int batchSize) {
        List<JsonObject> rows = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            rows.add(pendingOperations.get(i).getRow());
        }
        int writtenRows = 0;
        try {
            while (writtenRows < rows.size()) {
                writtenRows += writeUpsertChunk(collectionName, partitionName, rows, writtenRows);
            }
        } catch (RuntimeException e) {
            trimPendingOperationsOnPartialFailure(writtenRows, e);
            throw e;
        }
    }

    private void flushDeleteBatch(String collectionName, String partitionName, int batchSize) {
        List<Object> ids = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            ids.add(pendingOperations.get(i).getPrimaryKey());
        }
        int writtenIds = 0;
        try {
            while (writtenIds < ids.size()) {
                writtenIds += writeDeleteChunk(collectionName, partitionName, ids, writtenIds);
            }
        } catch (RuntimeException e) {
            trimPendingOperationsOnPartialFailure(writtenIds, e);
            throw e;
        }
    }

    private void trimPendingOperationsOnPartialFailure(int completedRows, RuntimeException error) {
        if (completedRows <= 0 || !MilvusRetryUtils.isRetryable(error)) {
            return;
        }
        pendingOperations.subList(0, completedRows).clear();
        metrics.setPendingRows(pendingOperations.size());
        LOG.warn(
                "Milvus batch partially flushed {} rows before failure. Remaining pending rows={}. ErrorCode={}.",
                completedRows,
                pendingOperations.size(),
                MilvusRetryUtils.classify(error),
                error);
    }

    private int writeUpsertChunk(
            String collectionName, String partitionName, List<JsonObject> rows, int startIndex) {
        ensurePartition(collectionName, partitionName);
        int remaining = rows.size() - startIndex;
        int chunkSize = remaining;
        while (chunkSize > 0) {
            List<JsonObject> chunk =
                    new ArrayList<>(rows.subList(startIndex, startIndex + chunkSize));
            try {
                client.upsert(config.getDatabaseName(), collectionName, partitionName, chunk);
                return chunkSize;
            } catch (RuntimeException e) {
                if (shouldSplitBatch(e, chunkSize)) {
                    int nextChunkSize = chunkSize / 2;
                    if (nextChunkSize <= 0) {
                        throw e;
                    }
                    chunkSize = nextChunkSize;
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Failed to write Milvus upsert chunk.");
    }

    private int writeDeleteChunk(
            String collectionName, String partitionName, List<Object> ids, int startIndex) {
        ensurePartition(collectionName, partitionName);
        int remaining = ids.size() - startIndex;
        int chunkSize = remaining;
        while (chunkSize > 0) {
            List<Object> chunk = new ArrayList<>(ids.subList(startIndex, startIndex + chunkSize));
            try {
                client.delete(config.getDatabaseName(), collectionName, partitionName, chunk);
                return chunkSize;
            } catch (RuntimeException e) {
                if (shouldSplitBatch(e, chunkSize)) {
                    int nextChunkSize = chunkSize / 2;
                    if (nextChunkSize <= 0) {
                        throw e;
                    }
                    chunkSize = nextChunkSize;
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Failed to write Milvus delete chunk.");
    }

    private int countLeadingBatchSize(List<MilvusOperation> operations) {
        MilvusOperation first = operations.get(0);
        int batchSize = 1;
        while (batchSize < operations.size()) {
            MilvusOperation operation = operations.get(batchSize);
            if (operation.getType() != first.getType()
                    || !first.getCollectionName().equals(operation.getCollectionName())
                    || !samePartition(first.getPartitionName(), operation.getPartitionName())) {
                break;
            }
            batchSize++;
        }
        return batchSize;
    }

    private void ensurePartition(String collectionName, String partitionName) {
        if (partitionName == null || partitionName.isEmpty()) {
            return;
        }
        if (!config.isPartitionAutoCreateEnabled()) {
            return;
        }
        String partitionKey = collectionName + "/" + partitionName;
        if (knownPartitions.contains(partitionKey)) {
            return;
        }
        if (!client.hasPartition(config.getDatabaseName(), collectionName, partitionName)) {
            try {
                ensurePartitionAutoCreateLimit(collectionName, partitionName);
                client.createPartition(config.getDatabaseName(), collectionName, partitionName);
            } catch (RuntimeException e) {
                if (MilvusRetryUtils.isPartitionAlreadyExistsError(e)
                        || client.hasPartition(
                                config.getDatabaseName(), collectionName, partitionName)) {
                    knownPartitions.add(partitionKey);
                    return;
                }
                throw e;
            }
        }
        knownPartitions.add(partitionKey);
    }

    private void ensurePartitionAutoCreateLimit(String collectionName, String partitionName) {
        if (knownPartitions.size() >= config.getPartitionAutoCreateMaxCount()) {
            throw new IllegalStateException(
                    "Milvus partition auto-create limit exceeded. collection="
                            + collectionName
                            + ", partition="
                            + partitionName
                            + ", limit="
                            + config.getPartitionAutoCreateMaxCount()
                            + ". Configure partition.names or increase partition.auto-create.max-count only after validating partition cardinality.");
        }
    }

    private void reconnectIfNeeded(RuntimeException failure) {
        if (!MilvusRetryUtils.shouldReconnect(failure)) {
            return;
        }
        LOG.warn(
                "Reconnecting Milvus client after failed flush. ErrorCode={}.",
                MilvusRetryUtils.classify(failure),
                failure);
        client.reconnect();
        metrics.recordReconnect();
    }

    private boolean shouldSplitBatch(RuntimeException e, int batchSize) {
        if (!config.isAdaptiveBatchSplitEnabled()
                || batchSize <= Math.max(1, config.getAdaptiveBatchSplitMinRows())) {
            return false;
        }
        return MilvusRetryUtils.isAdaptiveSplitError(e);
    }

    private boolean samePartition(String left, String right) {
        if (left == null) {
            return right == null;
        }
        return left.equals(right);
    }

    private void sleepBeforeRetry(int attempt) {
        long backoffMillis =
                MilvusRetryUtils.computeRetryBackoffMillis(
                        config.getRetryBackoff().toMillis(), attempt);
        if (backoffMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting to retry Milvus batch flush.", e);
        }
    }
}
