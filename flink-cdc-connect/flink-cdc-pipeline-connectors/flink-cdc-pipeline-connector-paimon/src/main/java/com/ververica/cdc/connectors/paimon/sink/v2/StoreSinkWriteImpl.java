/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.paimon.sink.v2;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * copy from {@link org.apache.paimon.flink.sink.StoreSinkWriteImpl}. remove {@link
 * StoreSinkWriteState} because we can't create it using {@link StateInitializationContext}.
 */
public class StoreSinkWriteImpl implements StoreSinkWrite {

    private static final Logger LOG =
            LoggerFactory.getLogger(org.apache.paimon.flink.sink.StoreSinkWriteImpl.class);

    protected final String commitUser;
    private final IOManagerImpl paimonIOManager;
    private final boolean ignorePreviousFiles;
    private final boolean waitCompaction;
    private final boolean isStreamingMode;
    @Nullable private final MemorySegmentPool memoryPool;
    @Nullable private final MemoryPoolFactory memoryPoolFactory;

    protected TableWriteImpl<?> write;

    @Nullable private final MetricGroup metricGroup;

    public StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        this(
                table,
                commitUser,
                ioManager,
                ignorePreviousFiles,
                waitCompaction,
                isStreamingMode,
                null,
                memoryPoolFactory,
                metricGroup);
    }

    private StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            @Nullable MemorySegmentPool memoryPool,
            @Nullable MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        this.commitUser = commitUser;
        this.paimonIOManager = new IOManagerImpl(ioManager.getSpillingDirectoriesPaths());
        this.ignorePreviousFiles = ignorePreviousFiles;
        this.waitCompaction = waitCompaction;
        this.isStreamingMode = isStreamingMode;
        this.memoryPool = memoryPool;
        this.memoryPoolFactory = memoryPoolFactory;
        this.metricGroup = metricGroup;
        this.write = newTableWrite(table);
    }

    private TableWriteImpl<?> newTableWrite(FileStoreTable table) {
        checkArgument(
                !(memoryPool != null && memoryPoolFactory != null),
                "memoryPool and memoryPoolFactory cannot be set at the same time.");

        TableWriteImpl<?> tableWrite =
                table.newWrite(commitUser, (part, bucket) -> true)
                        .withIOManager(paimonIOManager)
                        .withIgnorePreviousFiles(ignorePreviousFiles);

        if (metricGroup != null) {
            tableWrite.withMetricRegistry(new FlinkMetricRegistry(metricGroup));
        }

        if (memoryPoolFactory != null) {
            return tableWrite.withMemoryPoolFactory(memoryPoolFactory);
        } else {
            return tableWrite.withMemoryPool(
                    memoryPool != null
                            ? memoryPool
                            : new HeapMemorySegmentPool(
                                    table.coreOptions().writeBufferSize(),
                                    table.coreOptions().pageSize()));
        }
    }

    public void withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
    }

    @Override
    public SinkRecord write(InternalRow rowData) throws Exception {
        return write.writeAndReturn(rowData);
    }

    @Override
    public SinkRecord toLogRecord(SinkRecord record) {
        return write.toLogRecord(record);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Receive {} new files from snapshot {}, partition {}, bucket {}",
                    files.size(),
                    snapshotId,
                    partition,
                    bucket);
        }
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = new ArrayList<>();
        if (write != null) {
            try {
                for (CommitMessage committable :
                        write.prepareCommit(this.waitCompaction || waitCompaction, checkpointId)) {
                    committables.add(
                            new Committable(checkpointId, Committable.Kind.FILE, committable));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return committables;
    }

    @Override
    public void snapshotState() {
        // do nothing
    }

    @Override
    public boolean streamingMode() {
        return isStreamingMode;
    }

    @Override
    public void close() throws Exception {
        if (write != null) {
            write.close();
        }

        paimonIOManager.close();
    }

    @Override
    public void replace(FileStoreTable newTable) throws Exception {
        if (commitUser == null) {
            return;
        }
        write.close();
        write = newTableWrite(newTable);
    }
}
