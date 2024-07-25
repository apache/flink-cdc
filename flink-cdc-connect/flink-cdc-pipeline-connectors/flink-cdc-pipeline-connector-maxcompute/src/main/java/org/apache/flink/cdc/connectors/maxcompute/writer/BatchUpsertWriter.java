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

package org.apache.flink.cdc.connectors.maxcompute.writer;

import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.common.UncheckedOdpsException;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl.Builder;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * MaxCompute upsert writer, use {@link UpsertSessionImpl} and {@link UpsertStream} to write data.
 * Each session corresponds to a stream.
 */
public class BatchUpsertWriter implements MaxComputeWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BatchUpsertWriter.class);

    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final SessionIdentifier sessionIdentifier;
    private final TableTunnel tunnel;
    private UpsertSessionImpl upsertSession;
    private UpsertStream upsertStream;

    public BatchUpsertWriter(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            SessionIdentifier sessionIdentifier)
            throws IOException {
        this.options = options;
        this.writeOptions = writeOptions;

        this.tunnel = MaxComputeUtils.getTunnel(options, writeOptions);
        this.sessionIdentifier = sessionIdentifier;

        initOrReloadSession(sessionIdentifier);
    }

    private void initOrReloadSession(SessionIdentifier identifier) throws IOException {
        String partitionSpec = identifier.getPartitionName();
        String sessionId = identifier.getSessionId();
        try {
            this.upsertSession =
                    ((Builder)
                                    tunnel.buildUpsertSession(
                                            identifier.getProject(), identifier.getTable()))
                            .setConfig(tunnel.getConfig())
                            .setSchemaName(identifier.getSchema())
                            .setPartitionSpec(partitionSpec)
                            .setUpsertId(sessionId)
                            .setConcurrentNum(writeOptions.getFlushConcurrent())
                            .build();
            this.upsertStream =
                    upsertSession
                            .buildUpsertStream()
                            .setListener(new UpsertStreamListener(upsertSession))
                            .setMaxBufferSize(writeOptions.getMaxBufferSize())
                            .setSlotBufferSize(writeOptions.getSlotBufferSize())
                            .setCompressOption(
                                    MaxComputeUtils.compressOptionOf(
                                            writeOptions.getCompressAlgorithm()))
                            .build();
        } catch (OdpsException e) {
            throw new UncheckedOdpsException(e);
        }
    }

    @Override
    public SessionIdentifier getSessionIdentifier() {
        return sessionIdentifier;
    }

    @Override
    public ArrayRecord newElement() {
        return (ArrayRecord) upsertSession.newRecord();
    }

    @Override
    public void write(ArrayRecord record) throws IOException {
        try {
            upsertStream.upsert(record);
        } catch (OdpsException e) {
            throw new IOException(e.getMessage() + "RequestId: " + e.getRequestId(), e);
        }
    }

    @Override
    public void delete(ArrayRecord record) throws IOException {
        try {
            upsertStream.delete(record);
        } catch (OdpsException e) {
            throw new IOException(e.getMessage() + "RequestId: " + e.getRequestId(), e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            upsertStream.flush();
        } catch (OdpsException e) {
            throw new IOException(e.getMessage() + "RequestId: " + e.getRequestId(), e);
        }
    }

    @Override
    public String getId() {
        return upsertSession.getId();
    }

    @Override
    public void commit() throws IOException {
        try {
            upsertSession.commit(false);
            upsertSession.close();
        } catch (OdpsException e) {
            throw new IOException(e.getMessage() + "RequestId: " + e.getRequestId(), e);
        }
    }

    static class UpsertStreamListener extends UpsertSessionImpl.DefaultUpsertSteamListener {

        public UpsertStreamListener(UpsertSessionImpl session) {
            super(session);
        }

        @Override
        public void onFlush(UpsertStream.FlushResult result) {
            // metrics here
            LOG.info(
                    "Flush success, trace id: {}, time: {}, record: {}",
                    result.traceId,
                    result.flushTime,
                    result.recordCount);
        }

        @Override
        public boolean onFlushFail(Exception error, int retry) {
            LOG.error(
                    "Flush failed error {}, requestId {}",
                    error.getMessage(),
                    error instanceof TunnelException
                            ? ((TunnelException) error).getRequestId()
                            : "");
            return super.onFlushFail(error, retry);
        }
    }
}
