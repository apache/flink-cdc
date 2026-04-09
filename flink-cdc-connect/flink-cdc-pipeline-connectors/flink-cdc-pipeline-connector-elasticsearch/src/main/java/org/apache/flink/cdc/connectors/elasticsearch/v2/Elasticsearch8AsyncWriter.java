/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSinkWriterAdapter;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterAdapter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Elasticsearch8AsyncWriter Apache Flink's Async Sink Writer that submits Operations into an
 * Elasticsearch cluster.
 *
 * @param <InputT> type of Operations
 */
public class Elasticsearch8AsyncWriter<InputT> extends AsyncSinkWriterAdapter<InputT, Operation>
        implements StatefulSinkWriterAdapter<InputT, BufferedRequestState<Operation>> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8AsyncWriter.class);

    private final ElasticsearchAsyncClient esClient;

    private boolean close = false;

    private final Counter numRecordsOutErrorsCounter;

    /**
     * A counter to track number of records that are returned by Elasticsearch as failed and then
     * retried by this writer.
     */
    private final Counter numRecordsSendPartialFailureCounter;

    /** A counter to track the number of bulk requests that are sent to Elasticsearch. */
    private final Counter numRequestSubmittedCounter;

    private static final FatalExceptionClassifier ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    new FatalExceptionClassifier(
                            err ->
                                    err instanceof NoRouteToHostException
                                            || err instanceof ConnectException,
                            err ->
                                    new FlinkRuntimeException(
                                            "Could not connect to Elasticsearch cluster using the provided hosts",
                                            err)));

    public Elasticsearch8AsyncWriter(
            ElementConverter<InputT, Operation> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            NetworkConfig networkConfig,
            Collection<BufferedRequestState<Operation>> state) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                state);

        this.esClient = networkConfig.createEsClient();
        final SinkWriterMetricGroup metricGroup = context.metricGroup();
        checkNotNull(metricGroup);

        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsSendPartialFailureCounter =
                metricGroup.counter("numRecordsSendPartialFailure");
        this.numRequestSubmittedCounter = metricGroup.counter("numRequestSubmitted");
    }

    @Override
    protected void submitRequestEntries(
            List<Operation> requestEntries, ResultHandler<Operation> resultHandler) {
        numRequestSubmittedCounter.inc();
        LOG.debug("submitRequestEntries with {} items", requestEntries.size());

        BulkRequest.Builder br = new BulkRequest.Builder();
        boolean hasOperations = false;
        for (Operation operation : requestEntries) {
            if (operation.getBulkOperationVariant() == null) {
                continue;
            }
            br.operations(new BulkOperation(operation.getBulkOperationVariant()));
            hasOperations = true;
        }

        if (!hasOperations) {
            LOG.debug(
                    "Skipping empty BulkRequest, all {} operation(s) have null BulkOperationVariant",
                    requestEntries.size());
            resultHandler.complete();
            return;
        }

        esClient.bulk(br.build())
                .whenComplete(
                        (response, error) -> {
                            if (error != null) {
                                handleFailedRequest(requestEntries, resultHandler, error);
                            } else if (response.errors()) {
                                handlePartiallyFailedRequest(
                                        requestEntries, resultHandler, response);
                            } else {
                                handleSuccessfulRequest(resultHandler, response);
                            }
                        });
    }

    private void handleFailedRequest(
            List<Operation> requestEntries,
            ResultHandler<Operation> resultHandler,
            Throwable error) {
        LOG.warn(
                "The BulkRequest of {} operation(s) has failed due to: {}",
                requestEntries.size(),
                error.getMessage());
        LOG.debug("The BulkRequest has failed", error);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        Throwable retryableError = error.getCause() != null ? error.getCause() : error;
        if (isRetryable(retryableError)) {
            resultHandler.retryForEntries(requestEntries);
        }
    }

    private void handlePartiallyFailedRequest(
            List<Operation> requestEntries,
            ResultHandler<Operation> resultHandler,
            BulkResponse response) {
        LOG.debug("The BulkRequest has failed partially. Response: {}", response);
        ArrayList<Operation> failedItems = new ArrayList<>();
        for (int i = 0; i < response.items().size(); i++) {
            BulkResponseItem item = response.items().get(i);
            if (item.error() != null) {
                failedItems.add(requestEntries.get(i));

                LOG.error("Failed operation {}: {}", i, item.error().reason());
            }
        }

        numRecordsOutErrorsCounter.inc(failedItems.size());
        numRecordsSendPartialFailureCounter.inc(failedItems.size());
        LOG.info(
                "The BulkRequest with {} operation(s) has {} failure(s). It took {}ms",
                requestEntries.size(),
                failedItems.size(),
                response.took());
        resultHandler.retryForEntries(failedItems);
    }

    private void handleSuccessfulRequest(
            ResultHandler<Operation> resultHandler, BulkResponse response) {
        LOG.debug(
                "The BulkRequest of {} operation(s) completed successfully. It took {}ms",
                response.items().size(),
                response.took());
        resultHandler.complete();
    }

    private boolean isRetryable(Throwable error) {
        return !ELASTICSEARCH_FATAL_EXCEPTION_CLASSIFIER.isFatal(error, getFatalExceptionCons());
    }

    @Override
    protected long getSizeInBytes(Operation requestEntry) {
        return new OperationSerializer().size(requestEntry);
    }

    @Override
    public void close() {
        if (!close) {
            close = true;
            esClient.shutdown();
        }
    }
}
