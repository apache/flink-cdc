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
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BatchAppendWriter}. */
class BatchAppendWriterTest {

    private static final String PROJECT = "project";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final String PARTITION = "pt='partition-value'";
    private static final String SESSION_ID = "session-id";

    private MaxComputeOptions options;
    private MaxComputeWriteOptions writeOptions;
    private TableTunnel tunnel;
    private TableTunnel.UploadSession uploadSession;
    private MockedStatic<MaxComputeUtils> mockedMaxComputeUtils;

    @BeforeEach
    void setUp() throws Exception {
        options = mock(MaxComputeOptions.class);
        writeOptions = MaxComputeWriteOptions.builder().build();
        tunnel = mock(TableTunnel.class);
        uploadSession = mock(TableTunnel.UploadSession.class);
        RecordWriter recordWriter = mock(RecordWriter.class);
        CompressOption compressOption = mock(CompressOption.class);

        mockedMaxComputeUtils = Mockito.mockStatic(MaxComputeUtils.class);
        mockedMaxComputeUtils
                .when(() -> MaxComputeUtils.getTunnel(options, writeOptions))
                .thenReturn(tunnel);
        mockedMaxComputeUtils
                .when(() -> MaxComputeUtils.compressOptionOf(writeOptions.getCompressAlgorithm()))
                .thenReturn(compressOption);
        when(uploadSession.openBufferedWriter(compressOption)).thenReturn(recordWriter);
    }

    @AfterEach
    void tearDown() {
        mockedMaxComputeUtils.close();
    }

    @Test
    void testCreateSessionWithoutPartitionSpec() throws Exception {
        when(tunnel.createUploadSession(PROJECT, SCHEMA, TABLE, false)).thenReturn(uploadSession);

        new BatchAppendWriter(
                options, writeOptions, SessionIdentifier.of(PROJECT, SCHEMA, TABLE, null));

        verify(tunnel).createUploadSession(PROJECT, SCHEMA, TABLE, false);
        verify(tunnel, never())
                .createUploadSession(
                        eq(PROJECT), eq(SCHEMA), eq(TABLE), any(PartitionSpec.class), eq(false));
    }

    @Test
    void testCreateSessionWithPartitionSpec() throws Exception {
        when(tunnel.createUploadSession(
                        eq(PROJECT), eq(SCHEMA), eq(TABLE), any(PartitionSpec.class), eq(false)))
                .thenReturn(uploadSession);

        new BatchAppendWriter(
                options, writeOptions, SessionIdentifier.of(PROJECT, SCHEMA, TABLE, PARTITION));

        ArgumentCaptor<PartitionSpec> partitionCaptor =
                ArgumentCaptor.forClass(PartitionSpec.class);
        verify(tunnel)
                .createUploadSession(
                        eq(PROJECT), eq(SCHEMA), eq(TABLE), partitionCaptor.capture(), eq(false));
        assertThat(partitionCaptor.getValue().toString(true, true))
                .isEqualTo(new PartitionSpec(PARTITION).toString(true, true));
        verify(tunnel, never()).createUploadSession(PROJECT, SCHEMA, TABLE, false);
    }

    @Test
    void testReloadSessionWithoutPartitionSpec() throws Exception {
        when(tunnel.getUploadSession(PROJECT, SCHEMA, TABLE, SESSION_ID)).thenReturn(uploadSession);

        new BatchAppendWriter(
                options,
                writeOptions,
                SessionIdentifier.of(PROJECT, SCHEMA, TABLE, "  ", SESSION_ID));

        verify(tunnel).getUploadSession(PROJECT, SCHEMA, TABLE, SESSION_ID);
        verify(tunnel, never())
                .getUploadSession(
                        eq(PROJECT),
                        eq(SCHEMA),
                        eq(TABLE),
                        any(PartitionSpec.class),
                        eq(SESSION_ID));
    }

    @Test
    void testReloadSessionWithPartitionSpec() throws Exception {
        when(tunnel.getUploadSession(
                        eq(PROJECT),
                        eq(SCHEMA),
                        eq(TABLE),
                        any(PartitionSpec.class),
                        eq(SESSION_ID)))
                .thenReturn(uploadSession);

        new BatchAppendWriter(
                options,
                writeOptions,
                SessionIdentifier.of(PROJECT, SCHEMA, TABLE, PARTITION, SESSION_ID));

        ArgumentCaptor<PartitionSpec> partitionCaptor =
                ArgumentCaptor.forClass(PartitionSpec.class);
        verify(tunnel)
                .getUploadSession(
                        eq(PROJECT),
                        eq(SCHEMA),
                        eq(TABLE),
                        partitionCaptor.capture(),
                        eq(SESSION_ID));
        assertThat(partitionCaptor.getValue().toString(true, true))
                .isEqualTo(new PartitionSpec(PARTITION).toString(true, true));
        verify(tunnel, never()).getUploadSession(PROJECT, SCHEMA, TABLE, SESSION_ID);
    }
}
