/*
 *
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
 *
 */

package org.apache.flink.cdc.connectors.elasticsearch.v2;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * Elasticsearch8AsyncSink Apache Flink's Async Sink that submits Operations into an Elasticsearch
 * cluster.
 *
 * @param <InputT> type of records that will be converted into {@link Operation} see {@link
 *     Elasticsearch8AsyncSinkBuilder} on how to construct valid instances
 */
public class Elasticsearch8AsyncSink<InputT> extends AsyncSinkBase<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8AsyncSink.class);

    @VisibleForTesting protected final NetworkConfig networkConfig;

    protected Elasticsearch8AsyncSink(
            ElementConverter<InputT, Operation> converter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInByte,
            NetworkConfig networkConfig) {
        super(
                converter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInByte);

        this.networkConfig = networkConfig;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Operation>> createWriter(
            InitContext context) {
        return new Elasticsearch8AsyncWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                networkConfig,
                Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Operation>> restoreWriter(
            InitContext context, Collection<BufferedRequestState<Operation>> recoveredState) {
        return new Elasticsearch8AsyncWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                networkConfig,
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<Operation>> getWriterStateSerializer() {
        return new Elasticsearch8AsyncSinkSerializer();
    }
}
