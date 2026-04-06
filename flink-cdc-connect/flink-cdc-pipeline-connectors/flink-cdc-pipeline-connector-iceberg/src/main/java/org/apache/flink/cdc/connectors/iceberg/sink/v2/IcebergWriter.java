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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.HadoopConfUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.RowDataUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link SinkWriter} for Apache Iceberg. */
public class IcebergWriter
        implements CommittingSinkWriter<Event, WriteResultWrapper>,
                StatefulSinkWriter<Event, IcebergWriterState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergWriter.class);

    public static final String DEFAULT_FILE_FORMAT = "parquet";

    public static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

    private Map<TableId, RowDataTaskWriterFactory> writerFactoryMap;

    private Map<TableId, TaskWriter<RowData>> writerMap;

    private Map<TableId, TableSchemaWrapper> schemaMap;

    private final List<WriteResultWrapper> temporaryWriteResult;

    /** Per-table batch index; incremented on each schema-change flush, even when no writer exists. */
    private Map<TableId, Integer> tableBatchIndexMap;

    private Catalog catalog;

    private final int taskId;

    private final int attemptId;

    private final ZoneId zoneId;

    private long lastCheckpointId;

    private final String jobId;

    private final String operatorId;

    public IcebergWriter(
            Map<String, String> catalogOptions,
            int taskId,
            int attemptId,
            ZoneId zoneId,
            long lastCheckpointId,
            String jobId,
            String operatorId,
            Map<String, String> hadoopConfOptions) {
        Configuration configuration = HadoopConfUtils.createConfiguration(hadoopConfOptions);
        catalog =
                CatalogUtil.buildIcebergCatalog(
                        this.getClass().getSimpleName(), catalogOptions, configuration);
        writerFactoryMap = new HashMap<>();
        writerMap = new HashMap<>();
        schemaMap = new HashMap<>();
        tableBatchIndexMap = new HashMap<>();
        temporaryWriteResult = new ArrayList<>();
        this.taskId = taskId;
        this.attemptId = attemptId;
        this.zoneId = zoneId;
        this.lastCheckpointId = lastCheckpointId;
        this.jobId = jobId;
        this.operatorId = operatorId;
        LOGGER.info(
                "IcebergWriter created, taskId: {}, attemptId: {}, lastCheckpointId: {}, jobId: {}, operatorId: {}",
                taskId,
                attemptId,
                lastCheckpointId,
                jobId,
                operatorId);
    }

    @Override
    public List<IcebergWriterState> snapshotState(long checkpointId) {
        return Collections.singletonList(new IcebergWriterState(jobId, operatorId));
    }

    @Override
    public Collection<WriteResultWrapper> prepareCommit() throws IOException {
        List<WriteResultWrapper> list = new ArrayList<>();
        list.addAll(temporaryWriteResult);
        list.addAll(getWriteResult());
        temporaryWriteResult.clear();
        tableBatchIndexMap.clear();
        lastCheckpointId++;
        return list;
    }

    private RowDataTaskWriterFactory getRowDataTaskWriterFactory(TableId tableId) {
        Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
        RowType rowType = FlinkSchemaUtil.convert(table.schema());
        RowDataTaskWriterFactory rowDataTaskWriterFactory =
                new RowDataTaskWriterFactory(
                        table,
                        rowType,
                        DEFAULT_MAX_FILE_SIZE,
                        FileFormat.fromString(DEFAULT_FILE_FORMAT),
                        new HashMap<>(),
                        new ArrayList<>(table.schema().identifierFieldIds()),
                        true);
        rowDataTaskWriterFactory.initialize(taskId, attemptId);
        return rowDataTaskWriterFactory;
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();
            writerFactoryMap.computeIfAbsent(tableId, this::getRowDataTaskWriterFactory);
            TaskWriter<RowData> writer =
                    writerMap.computeIfAbsent(
                            tableId, tableId1 -> writerFactoryMap.get(tableId1).create());
            TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);
            RowData rowData =
                    RowDataUtils.convertDataChangeEventToRowData(
                            dataChangeEvent, tableSchemaWrapper.getFieldGetters());
            writer.write(rowData);
        } else {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            // Flush only when the table is already known; skip on initial CreateTableEvent since
            // no data has been written yet and there is nothing to split.
            if (schemaMap.containsKey(tableId)) {
                flushTableWriter(tableId);
            }
            TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);

            Schema newSchema =
                    tableSchemaWrapper != null
                            ? SchemaUtils.applySchemaChangeEvent(
                                    tableSchemaWrapper.getSchema(), schemaChangeEvent)
                            : SchemaUtils.applySchemaChangeEvent(null, schemaChangeEvent);
            schemaMap.put(tableId, new TableSchemaWrapper(newSchema, zoneId));
        }
    }

    @Override
    public void flush(boolean flush) {
        // Flush may be called many times during one checkpoint by non-data events.
        // Avoid rotating all task writers here, which can split same-PK updates into multiple
        // batches within one checkpoint and break dedup semantics in downstream reads.
    }

    private void flushTableWriter(TableId tableId) throws IOException {
        TaskWriter<RowData> writer = writerMap.remove(tableId);
        // Advance even when no writer exists, to keep batchIndex in sync across subtasks.
        int batchIndex = tableBatchIndexMap.getOrDefault(tableId, 0);
        tableBatchIndexMap.put(tableId, batchIndex + 1);
        if (writer == null) {
            return;
        }
        WriteResultWrapper writeResultWrapper =
                new WriteResultWrapper(
                        writer.complete(),
                        tableId,
                        lastCheckpointId + 1,
                        jobId,
                        operatorId,
                        batchIndex);
        temporaryWriteResult.add(writeResultWrapper);
        LOGGER.info(writeResultWrapper.buildDescription());
        writerFactoryMap.remove(tableId);
    }

    private List<WriteResultWrapper> getWriteResult() throws IOException {
        long currentCheckpointId = lastCheckpointId + 1;
        List<WriteResultWrapper> writeResults = new ArrayList<>();
        for (Map.Entry<TableId, TaskWriter<RowData>> entry : writerMap.entrySet()) {
            TableId tableId = entry.getKey();
            int batchIndex = tableBatchIndexMap.getOrDefault(tableId, 0);
            WriteResultWrapper writeResultWrapper =
                    new WriteResultWrapper(
                            entry.getValue().complete(),
                            tableId,
                            currentCheckpointId,
                            jobId,
                            operatorId,
                            batchIndex);
            writeResults.add(writeResultWrapper);
            LOGGER.info(writeResultWrapper.buildDescription());
        }
        writerMap.clear();
        writerFactoryMap.clear();
        return writeResults;
    }

    @Override
    public void writeWatermark(Watermark watermark) {}

    @Override
    public void close() throws Exception {
        if (schemaMap != null) {
            schemaMap.clear();
            schemaMap = null;
        }

        if (writerMap != null) {
            for (TaskWriter<RowData> writer : writerMap.values()) {
                writer.close();
            }
            writerMap.clear();
            writerMap = null;
        }

        if (writerFactoryMap != null) {
            writerFactoryMap.clear();
            writerFactoryMap = null;
        }

        if (tableBatchIndexMap != null) {
            tableBatchIndexMap.clear();
            tableBatchIndexMap = null;
        }

        catalog = null;
    }
}
