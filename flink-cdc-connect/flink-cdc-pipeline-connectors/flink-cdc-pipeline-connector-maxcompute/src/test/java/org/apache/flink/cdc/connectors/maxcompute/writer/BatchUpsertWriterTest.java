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
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl.Builder;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BatchUpsertWriter}. */
class BatchUpsertWriterTest {

    private static final String PROJECT = "project";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final String PARTITION = "pt='partition-value'";
    private static final String SESSION_ID = "session-id";

    private MaxComputeOptions options;
    private MaxComputeWriteOptions writeOptions;
    private TableTunnel tunnel;
    private Builder sessionBuilder;
    private MockedStatic<MaxComputeUtils> mockedMaxComputeUtils;

    @BeforeEach
    void setUp() throws Exception {
        options = mock(MaxComputeOptions.class);
        writeOptions = MaxComputeWriteOptions.builder().build();
        tunnel = mock(TableTunnel.class);
        sessionBuilder = mock(Builder.class);
        UpsertSessionImpl upsertSession = mock(UpsertSessionImpl.class);
        UpsertStream.Builder streamBuilder = mock(UpsertStream.Builder.class);
        UpsertStream upsertStream = mock(UpsertStream.class);
        Configuration configuration = mock(Configuration.class);
        CompressOption compressOption = mock(CompressOption.class);

        mockedMaxComputeUtils = Mockito.mockStatic(MaxComputeUtils.class);
        mockedMaxComputeUtils
                .when(() -> MaxComputeUtils.getTunnel(options, writeOptions))
                .thenReturn(tunnel);
        mockedMaxComputeUtils
                .when(() -> MaxComputeUtils.compressOptionOf(writeOptions.getCompressAlgorithm()))
                .thenReturn(compressOption);

        when(tunnel.buildUpsertSession(PROJECT, TABLE)).thenReturn(sessionBuilder);
        when(tunnel.getConfig()).thenReturn(configuration);
        when(sessionBuilder.setConfig(configuration)).thenReturn(sessionBuilder);
        when(sessionBuilder.setSchemaName(SCHEMA)).thenReturn(sessionBuilder);
        when(sessionBuilder.setPartitionSpec(anyString())).thenReturn(sessionBuilder);
        when(sessionBuilder.setUpsertId(SESSION_ID)).thenReturn(sessionBuilder);
        when(sessionBuilder.setConcurrentNum(writeOptions.getFlushConcurrent()))
                .thenReturn(sessionBuilder);
        when(sessionBuilder.build()).thenReturn(upsertSession);

        when(upsertSession.buildUpsertStream()).thenReturn(streamBuilder);
        when(streamBuilder.setListener(any(UpsertStream.Listener.class))).thenReturn(streamBuilder);
        when(streamBuilder.setMaxBufferSize(writeOptions.getMaxBufferSize()))
                .thenReturn(streamBuilder);
        when(streamBuilder.setSlotBufferSize(writeOptions.getSlotBufferSize()))
                .thenReturn(streamBuilder);
        when(streamBuilder.setCompressOption(compressOption)).thenReturn(streamBuilder);
        when(streamBuilder.build()).thenReturn(upsertStream);
    }

    @AfterEach
    void tearDown() {
        mockedMaxComputeUtils.close();
    }

    @Test
    void testDoesNotSetNullOrBlankPartitionSpec() throws Exception {
        new BatchUpsertWriter(
                options,
                writeOptions,
                SessionIdentifier.of(PROJECT, SCHEMA, TABLE, null, SESSION_ID));
        new BatchUpsertWriter(
                options,
                writeOptions,
                SessionIdentifier.of(PROJECT, SCHEMA, TABLE, "  ", SESSION_ID));

        verify(sessionBuilder, never()).setPartitionSpec(nullable(String.class));
        verify(sessionBuilder, never()).setPartitionSpec(any(PartitionSpec.class));
        verify(sessionBuilder, times(2)).build();
    }

    @Test
    void testSetsNonBlankPartitionSpec() throws Exception {
        new BatchUpsertWriter(
                options,
                writeOptions,
                SessionIdentifier.of(PROJECT, SCHEMA, TABLE, PARTITION, SESSION_ID));

        verify(sessionBuilder).setPartitionSpec(PARTITION);
        verify(sessionBuilder, never()).setPartitionSpec(any(PartitionSpec.class));
    }
}
