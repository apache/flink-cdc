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
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeExecutionOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;
import org.apache.flink.cdc.connectors.maxcompute.utils.RetryUtils;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.impl.ConfigurationImpl;
import com.aliyun.odps.tunnel.impl.UpsertRecord;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl.Builder;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.tunnel.streams.UpsertStream.FlushResult;
import com.aliyun.odps.tunnel.streams.UpsertStream.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * MaxCompute upsert writer, use {@link UpsertSessionImpl} and {@link UpsertStream} to write data.
 * Each session corresponds to a stream.
 */
public class MaxComputeUpsertWriter implements MaxComputeWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeUpsertWriter.class);

    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final MaxComputeExecutionOptions executionOptions;
    private final SessionIdentifier sessionIdentifier;
    private final TableTunnel tunnel;
    private UpsertSessionImpl upsertSession;
    private UpsertStream upsertStream;

    public MaxComputeUpsertWriter(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            MaxComputeExecutionOptions executionOptions,
            SessionIdentifier sessionIdentifier)
            throws IOException {
        this.options = options;
        this.writeOptions = writeOptions;
        this.executionOptions = executionOptions;

        this.tunnel = MaxComputeUtils.getTunnel(options);
        this.sessionIdentifier = sessionIdentifier;

        LOG.info("sink writer reload session: {}", sessionIdentifier);
        reloadSession(sessionIdentifier);
    }

    private void reloadSession(SessionIdentifier identifier) throws IOException {
        String partitionSpec = identifier.getPartitionName();
        String sessionId = identifier.getSessionId();
        Listener listener =
                new Listener() {
                    @Override
                    public void onFlush(FlushResult result) {
                        // metrics here
                    }

                    @Override
                    public boolean onFlushFail(String error, int retry) {
                        LOG.error("Flush failed error: {}", error);
                        if (retry > executionOptions.getMaxRetries()) {
                            return false;
                        }

                        try {
                            if (error.contains("Quota Exceeded")
                                    || error.contains("SlotExceeded")) {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(5000));
                            } else {
                                Thread.sleep(1000);
                            }
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                        LOG.warn("Start to retry, retryCount: {}", retry);
                        return true;
                    }
                };

        RetryUtils.execute(
                () -> {
                    this.upsertSession =
                            ((Builder)
                                            tunnel.buildUpsertSession(
                                                    identifier.getProject(), identifier.getTable()))
                                    .setConfig((ConfigurationImpl) tunnel.getConfig())
                                    .setSchemaName(
                                            options.isSupportSchema()
                                                    ? identifier.getSchema()
                                                    : null)
                                    .setPartitionSpec(partitionSpec)
                                    .setUpsertId(sessionId)
                                    .setConcurrentNum(writeOptions.getFlushConcurrent())
                                    .build();
                    this.upsertStream =
                            upsertSession
                                    .buildUpsertStream()
                                    .setListener(listener)
                                    .setMaxBufferSize(writeOptions.getMaxBufferSize())
                                    .setSlotBufferSize(writeOptions.getSlotBufferSize())
                                    .setCompressOption(
                                            MaxComputeUtils.compressOptionOf(
                                                    writeOptions.getCompressAlgorithm()))
                                    .build();
                    return null;
                },
                executionOptions.getMaxRetries(),
                executionOptions.getRetryIntervalMillis());
    }

    @Override
    public SessionIdentifier getSessionIdentifier() {
        return sessionIdentifier;
    }

    @Override
    public UpsertRecord newElement() {
        return (UpsertRecord) upsertSession.newRecord();
    }

    @Override
    public void write(UpsertRecord record) throws IOException {
        try {
            upsertStream.upsert(record);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(UpsertRecord record) throws IOException {
        try {
            upsertStream.delete(record);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            upsertStream.flush();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getId() {
        return upsertSession.getId();
    }
}
