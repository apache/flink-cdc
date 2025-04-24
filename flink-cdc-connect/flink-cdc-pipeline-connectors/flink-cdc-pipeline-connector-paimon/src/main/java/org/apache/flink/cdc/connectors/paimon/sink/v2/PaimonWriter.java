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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private static final Logger LOG = LoggerFactory.getLogger(PaimonWriter.class);

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
        long startTime = System.currentTimeMillis();
        List<MultiTableCommittable> committables =
                writes.entrySet()
                        .parallelStream()
                        .flatMap(
                                entry -> {
                                    try {
                                        // here we set it to lastCheckpointId+1 to
                                        // avoid prepareCommit the same checkpointId with the first
                                        // round.
                                        return entry.getValue()
                                                .prepareCommit(false, lastCheckpointId + 1).stream()
                                                .map(
                                                        committable ->
                                                                MultiTableCommittable
                                                                        .fromCommittable(
                                                                                entry.getKey(),
                                                                                committable));
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(Collectors.toList());
        lastCheckpointId++;
        LOG.debug("Spend {} ms to prepareCommit", System.currentTimeMillis() - startTime);
        return committables;
    }

    @Override
    public void write(InputT event, Context context) throws IOException {
        PaimonEvent paimonEvent = serializer.serialize(event);
        Identifier tableId = paimonEvent.getTableId();
        if (paimonEvent.isShouldRefreshSchema()) {
            // remove the table temporarily, then add the table with latest schema when received
            // DataChangeEvent.
            tables.remove(tableId);
            try {
                if (writes.containsKey(tableId)) {
                    writes.get(tableId).replace(getTable(tableId));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (paimonEvent.getGenericRows() != null) {
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
                                boolean waitCompaction =
                                        Boolean.parseBoolean(
                                                table.options()
                                                        .getOrDefault(
                                                                CoreOptions.DELETION_VECTORS_ENABLED
                                                                        .key(),
                                                                CoreOptions.DELETION_VECTORS_ENABLED
                                                                        .defaultValue()
                                                                        .toString()));
                                StoreSinkWriteImpl storeSinkWrite =
                                        new StoreSinkWriteImpl(
                                                table,
                                                commitUser,
                                                ioManager,
                                                false,
                                                waitCompaction,
                                                true,
                                                memoryPoolFactory,
                                                metricGroup);
                                storeSinkWrite.withCompactExecutor(compactExecutor);
                                return storeSinkWrite;
                            });
            try {
                for (GenericRow genericRow : paimonEvent.getGenericRows()) {
                    write.write(genericRow, paimonEvent.getBucket());
                }
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

    @Override
    public void flush(boolean endOfInput) {
        // do nothing as StoreSinkWrite#replace will write buffer to file.
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
