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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;

import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.utils.SchemaUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorThreadFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/** A {@link Sink} to write {@link DataChangeEvent} to Paimon storage. */
public class PaimonWriter
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<Event, MultiTableCommittable> {

    // use `static` because Catalog is unSerializable.
    private static Catalog catalog;
    private final IOManager ioManager;

    // maintain the latest schema of tableId
    private final Map<TableId, TableSchemaInfo> schemaMaps;
    // Each job can only have one user name and this name must be consistent across restarts.
    private final String commitUser;
    // all table write should share one write buffer so that writers can preempt memory
    // from those of other tables
    private MemoryPoolFactory memoryPoolFactory;
    private final Map<Identifier, FileStoreTable> tables;
    private final Map<Identifier, StoreSinkWrite> writes;
    private final ExecutorService compactExecutor;
    private final MetricGroup metricGroup;
    private final ZoneId zoneId;

    public PaimonWriter(
            Options catalogOptions, MetricGroup metricGroup, ZoneId zoneId, String commitUser) {
        catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        this.metricGroup = metricGroup;
        this.commitUser = commitUser;
        this.tables = new HashMap<>();
        this.writes = new HashMap<>();
        this.schemaMaps = new HashMap<>();
        this.ioManager = new IOManagerAsync();
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-CdcMultiWrite-Compaction"));
        this.zoneId = zoneId;
    }

    @Override
    public Collection<MultiTableCommittable> prepareCommit() throws IOException {
        List<MultiTableCommittable> committables = new LinkedList<>();
        for (Map.Entry<Identifier, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier key = entry.getKey();
            StoreSinkWrite write = entry.getValue();
            boolean waitCompaction = false;
            // checkpointId will be updated correctly by PreCommitOperator.
            long checkpointId = 1L;
            committables.addAll(
                    write.prepareCommit(waitCompaction, checkpointId).stream()
                            .map(
                                    committable ->
                                            MultiTableCommittable.fromCommittable(key, committable))
                            .collect(Collectors.toList()));
        }
        return committables;
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        if (event instanceof SchemaChangeEvent) {
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schemaMaps.put(
                        createTableEvent.tableId(),
                        new TableSchemaInfo(createTableEvent.getSchema(), zoneId));
            } else {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                schemaMaps.put(
                        schemaChangeEvent.tableId(),
                        new TableSchemaInfo(
                                SchemaUtils.applySchemaChangeEvent(
                                        schemaMaps.get(schemaChangeEvent.tableId()).getSchema(),
                                        schemaChangeEvent),
                                zoneId));

                Identifier tableId =
                        Identifier.create(
                                schemaChangeEvent.tableId().getSchemaName(),
                                schemaChangeEvent.tableId().getTableName());
                // fresh table in cache to get the latest schema.
                FileStoreTable table = getTable(tableId, true);
                writes.put(
                        tableId,
                        new StoreSinkWriteImpl(
                                table,
                                commitUser,
                                ioManager,
                                false,
                                false,
                                true,
                                memoryPoolFactory,
                                metricGroup));
            }
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            Identifier tableId =
                    Identifier.create(
                            dataChangeEvent.tableId().getSchemaName(),
                            dataChangeEvent.tableId().getTableName());

            FileStoreTable table;
            table = getTable(tableId, false);

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

            GenericRow genericRow =
                    PaimonWriterHelper.convertEventToGenericRow(
                            dataChangeEvent,
                            schemaMaps.get(dataChangeEvent.tableId()).getFieldGetters());
            try {
                write.write(genericRow);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private FileStoreTable getTable(Identifier tableId, boolean refresh) {
        FileStoreTable table = tables.get(tableId);
        if (table == null || refresh) {
            try {
                table = (FileStoreTable) catalog.getTable(tableId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            tables.put(tableId, table);
        }

        if (table.bucketMode() != BucketMode.FIXED) {
            throw new UnsupportedOperationException(
                    "Unified Sink only supports FIXED bucket mode, but is " + table.bucketMode());
        }
        return table;
    }

    @Override
    public void flush(boolean endOfInput) {}

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
