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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/** A {@link Sink} to write {@link DataChangeEvent} to Paimon storage. */
public class PaimonWriter<InputT>
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, MultiTableCommittable> {

    // use `static` because Catalog is unSerializable.
    private static Catalog catalog;
    private final IOManager ioManager;

    // Each job can only have one user name and this name must be consistent across restarts.
    private final String commitUser;
    // all table write should share one write buffer so that writers can preempt memory
    // from those of other tables
    private MemoryPoolFactory memoryPoolFactory;

    // deserializer that converts Input into PaimonEvent.
    private final PaimonRecordSerializer<InputT> serializer;
    private final Map<Identifier, FileStoreTable> tables;
    private final Map<Identifier, StoreSinkWrite> writes;
    private final ExecutorService compactExecutor;
    private final MetricGroup metricGroup;
    private final List<MultiTableCommittable> committables;

    /** A workaround variable trace the checkpointId in {@link StreamOperator#snapshotState}. */
    private long lastCheckpointId;

    public PaimonWriter(
            Options catalogOptions,
            MetricGroup metricGroup,
            String commitUser,
            PaimonRecordSerializer<InputT> serializer,
            long lastCheckpointId) {
        catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        this.metricGroup = metricGroup;
        this.commitUser = commitUser;
        this.tables = new HashMap<>();
        this.writes = new HashMap<>();
        this.committables = new ArrayList<>();
        this.ioManager = new IOManagerAsync();
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-CdcMultiWrite-Compaction"));
        this.serializer = serializer;
        this.lastCheckpointId = lastCheckpointId;
    }

    @Override
    public Collection<MultiTableCommittable> prepareCommit() {
        Collection<MultiTableCommittable> allCommittables = new ArrayList<>(committables);
        committables.clear();
        lastCheckpointId++;
        return allCommittables;
    }

    @Override
    public void write(InputT event, Context context) throws IOException {
        PaimonEvent paimonEvent = serializer.serialize(event);
        Identifier tableId = paimonEvent.getTableId();
        if (paimonEvent.isShouldRefreshSchema()) {
            // remove the table temporarily, then add the table with latest schema when received
            // DataChangeEvent.
            writes.remove(tableId);
            tables.remove(tableId);
        }
        if (paimonEvent.getGenericRow() != null) {
            FileStoreTable table;
            table = getTable(tableId);
            if (memoryPoolFactory == null) {
                memoryPoolFactory =
                        new MemoryPoolFactory(
                                // currently, the options of all tables are the same in CDC
                                new HeapMemorySegmentPool(
                                        table.coreOptions().writeBufferSize(),
                                        table.coreOptions().pageSize()));
            }
            StoreSinkWrite write =
                    writes.computeIfAbsent(
                            tableId,
                            id -> {
                                StoreSinkWriteImpl storeSinkWrite =
                                        new StoreSinkWriteImpl(
                                                table,
                                                commitUser,
                                                ioManager,
                                                false,
                                                false,
                                                true,
                                                memoryPoolFactory,
                                                metricGroup);
                                storeSinkWrite.withCompactExecutor(compactExecutor);
                                return storeSinkWrite;
                            });
            try {
                write.write(paimonEvent.getGenericRow(), paimonEvent.getBucket());
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private FileStoreTable getTable(Identifier tableId) {
        return tables.computeIfAbsent(
                tableId,
                id -> {
                    try {
                        return (FileStoreTable) catalog.getTable(tableId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Called on checkpoint or end of input so that the writer to flush all pending data for
     * at-least-once.
     *
     * <p>Execution order: flush(boolean endOfInput)=>prepareCommit()=>snapshotState(long
     * checkpointId).
     *
     * <p>this method will also be called when receiving {@link FlushEvent}, but we don't need to
     * commit the MultiTableCommittables immediately in this case, because {@link PaimonCommitter}
     * support committing data of different schemas.
     */
    @Override
    public void flush(boolean endOfInput) throws IOException {
        for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier key = entry.getKey();
            StoreSinkWrite write = entry.getValue();
            boolean waitCompaction = false;
            committables.addAll(
                    // here we set it to lastCheckpointId+1 to
                    // avoid prepareCommit the same checkpointId with the first round.
                    // One thing to note is that during schema evolution, flush and checkpoint are
                    // consistent,
                    // but as long as there is data coming in, it will not trigger any conflict
                    // issues
                    write.prepareCommit(waitCompaction, lastCheckpointId + 1).stream()
                            .map(
                                    committable ->
                                            MultiTableCommittable.fromCommittable(key, committable))
                            .collect(Collectors.toList()));
        }
    }

    @Override
    public void close() throws Exception {
        for (StoreSinkWrite write : writes.values()) {
            write.close();
        }
        if (compactExecutor != null) {
            compactExecutor.shutdownNow();
        }
    }
}
