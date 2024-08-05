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

import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.common.UncheckedOdpsException;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * MaxCompute upsert writer, use {@link UpsertSessionImpl} and {@link UpsertStream} to write data.
 * Each session corresponds to a stream.
 */
public class BatchAppendWriter implements MaxComputeWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BatchAppendWriter.class);

    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final SessionIdentifier sessionIdentifier;
    private final TableTunnel tunnel;
    private TableTunnel.UploadSession uploadSession;
    private RecordWriter recordWriter;

    public BatchAppendWriter(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            SessionIdentifier sessionIdentifier) {
        this.options = options;
        this.writeOptions = writeOptions;

        this.tunnel = MaxComputeUtils.getTunnel(options, writeOptions);
        this.sessionIdentifier = sessionIdentifier;

        LOG.info("sink writer reload session: {}", sessionIdentifier);
        initOrReloadSession(sessionIdentifier);
    }

    private void initOrReloadSession(SessionIdentifier identifier) {
        String partitionSpec = identifier.getPartitionName();
        String sessionId = identifier.getSessionId();

        try {
            if (StringUtils.isNullOrWhitespaceOnly(identifier.getSessionId())) {
                this.uploadSession =
                        tunnel.createUploadSession(
                                identifier.getProject(),
                                identifier.getSchema(),
                                identifier.getTable(),
                                new PartitionSpec(partitionSpec),
                                false);
            } else {
                this.uploadSession =
                        tunnel.getUploadSession(
                                identifier.getProject(),
                                identifier.getSchema(),
                                identifier.getTable(),
                                new PartitionSpec(partitionSpec),
                                sessionId);
            }
            this.recordWriter =
                    uploadSession.openBufferedWriter(
                            MaxComputeUtils.compressOptionOf(writeOptions.getCompressAlgorithm()));
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
        return (ArrayRecord) uploadSession.newRecord();
    }

    @Override
    public void write(ArrayRecord record) throws IOException {
        recordWriter.write(record);
    }

    @Override
    public void delete(ArrayRecord record) throws IOException {
        // append writer does not support delete. just ignore delete operation.
    }

    @Override
    public void flush() throws IOException {
        ((TunnelBufferedWriter) recordWriter).flush();
    }

    @Override
    public String getId() {
        return uploadSession.getId();
    }

    @Override
    public void commit() throws IOException {
        try {
            uploadSession.commit();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }
}
