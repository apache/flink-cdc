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

package org.apache.flink.connector.base.sink;

import org.apache.flink.api.connector.sink2.StatefulSinkWriterAdapter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * Compatibility adapter for Flink 1.20. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
public abstract class AsyncSinkBaseAdapter<InputT, RequestEntryT extends Serializable>
        extends AsyncSinkBase<InputT, RequestEntryT> {
    protected AsyncSinkBaseAdapter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<RequestEntryT>> restoreWriter(
            WriterInitContext context,
            Collection<BufferedRequestState<RequestEntryT>> recoveredState)
            throws IOException {
        return restoreWriterAdapter(context, recoveredState);
    }

    public abstract StatefulSinkWriterAdapter<InputT, BufferedRequestState<RequestEntryT>>
            restoreWriterAdapter(
                    WriterInitContext context,
                    Collection<BufferedRequestState<RequestEntryT>> recoveredState)
                    throws IOException;
}
