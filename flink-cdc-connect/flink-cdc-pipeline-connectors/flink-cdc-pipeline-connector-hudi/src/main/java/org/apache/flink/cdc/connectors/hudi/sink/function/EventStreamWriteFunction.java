/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.hudi.sink.function;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.avro.Schema;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.FlinkStreamWriteMetrics;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.buffer.RowDataBucket;
import org.apache.hudi.sink.buffer.TotalSizeTracer;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.exception.MemoryPagesExhaustedException;
import org.apache.hudi.sink.transform.RecordConverter;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.MutableIteratorWrapperIterator;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/** Base infrastructures for streaming writer function to handle Events. */
public abstract class EventStreamWriteFunction extends AbstractStreamWriteFunction<Event>
        implements EventProcessorFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamWriteFunction.class);

    /** Write buffer as buckets for a checkpoint. The key is bucket ID (partition path + fileID). */
    protected transient Map<String, RowDataBucket> buckets;

    /** Write function to trigger the actual write action. */
    protected transient WriteFunction writeFunction;

    private transient BufferedRecordMerger<RowData> recordMerger;
    private transient HoodieReaderContext<RowData> readerContext;
    private transient List<String> orderingFieldNames;

    protected RowType rowType;

    protected final RowDataKeyGen keyGen;

    /** Total size tracer. */
    private transient TotalSizeTracer tracer;

    /** Metrics for flink stream write. */
    protected transient FlinkStreamWriteMetrics writeMetrics;

    /** Table ID for table-specific coordination requests. */
    protected TableId tableId;

    protected transient MemorySegmentPool memorySegmentPool;

    protected transient RecordConverter recordConverter;

    /**
     * Constructs an EventStreamWriteFunction.
     *
     * @param config The config options
     */
    public EventStreamWriteFunction(Configuration config, RowType rowType) {
        super(config);
        this.rowType = rowType;
        this.keyGen = RowDataKeyGen.instance(config, rowType);
    }

    /**
     * Sets the table ID for this function. This is used for table-specific coordination requests.
     *
     * @param tableId The table ID
     */
    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        this.tracer = new TotalSizeTracer(this.config);
        initBuffer();
        initWriteFunction();
        initMergeClass();
        initRecordConverter();
        initWriteMetrics();
    }

    @Override
    public void snapshotState() {
        // Based on the fact that the coordinator starts the checkpoint first,
        // it would check the validity.
        // wait for the buffer data flush out and request a new instant
        LOG.info("Triggered snapshotState");
        flushRemaining(false);
    }

    @Override
    public final void processElement(
            Event event, ProcessFunction<Event, RowData>.Context ctx, Collector<RowData> out)
            throws Exception {
        // Route event to appropriate handler based on type
        if (event instanceof DataChangeEvent) {
            processDataChange((DataChangeEvent) event);
        } else if (event instanceof SchemaChangeEvent) {
            processSchemaChange((SchemaChangeEvent) event);
        } else if (event instanceof FlushEvent) {
            processFlush((FlushEvent) event);
        } else {
            LOG.warn("Received unknown event type: {}", event.getClass().getName());
        }
    }

    /**
     * Process data change events (INSERT/UPDATE/DELETE). This is where actual data is buffered and
     * written.
     *
     * <p>Implements {@link EventProcessorFunction#processDataChange(DataChangeEvent)}.
     *
     * @param event The data change event
     */
    @Override
    public abstract void processDataChange(DataChangeEvent event) throws Exception;

    /**
     * Process schema change events (CREATE TABLE, ADD COLUMN, etc.). Default: No-op. Override if
     * schema evolution is needed.
     *
     * <p>Implements {@link EventProcessorFunction#processSchemaChange(SchemaChangeEvent)}.
     *
     * @param event The schema change event
     */
    @Override
    public void processSchemaChange(SchemaChangeEvent event) throws Exception {
        LOG.debug("Schema change event not handled by {}: {}", getClass().getSimpleName(), event);
    }

    /**
     * Process flush events for coordinated flushing. Default: Flush all buffered data.
     *
     * <p>Implements {@link EventProcessorFunction#processFlush(FlushEvent)}.
     *
     * @param event The flush event
     */
    @Override
    public void processFlush(FlushEvent event) throws Exception {
        LOG.info("Received a flush event, flushing all remaining data.");
        flushRemaining(false);
    }

    @Override
    public void close() {
        if (this.writeClient != null) {
            this.writeClient.close();
        }
    }

    /** End input action for batch source. */
    public void endInput() {
        super.endInput();
        flushRemaining(true);
        this.writeClient.cleanHandles();
        this.writeStatuses.clear();
    }

    // -------------------------------------------------------------------------
    //  Utilities
    // -------------------------------------------------------------------------

    private void initBuffer() {
        this.buckets = new LinkedHashMap<>();
        this.memorySegmentPool = MemorySegmentPoolFactory.createMemorySegmentPool(config);
    }

    private void initWriteFunction() {
        final String writeOperation = this.config.get(FlinkOptions.OPERATION);
        switch (WriteOperationType.fromValue(writeOperation)) {
            case INSERT:
                this.writeFunction =
                        (records, bucketInfo, instantTime) ->
                                this.writeClient.insert(records, bucketInfo, instantTime);
                break;
            case UPSERT:
            case DELETE: // shares the code path with UPSERT
            case DELETE_PREPPED:
                this.writeFunction =
                        (records, bucketInfo, instantTime) ->
                                this.writeClient.upsert(records, bucketInfo, instantTime);
                break;
            case INSERT_OVERWRITE:
                this.writeFunction =
                        (records, bucketInfo, instantTime) ->
                                this.writeClient.insertOverwrite(records, bucketInfo, instantTime);
                break;
            case INSERT_OVERWRITE_TABLE:
                this.writeFunction =
                        (records, bucketInfo, instantTime) ->
                                this.writeClient.insertOverwriteTable(
                                        records, bucketInfo, instantTime);
                break;
            default:
                throw new RuntimeException("Unsupported write operation : " + writeOperation);
        }
    }

    private void initWriteMetrics() {
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        this.writeMetrics = new FlinkStreamWriteMetrics(metrics);
        this.writeMetrics.registerMetrics();
    }

    private void initRecordConverter() {
        this.recordConverter = RecordConverter.getInstance(keyGen);
    }

    private void initMergeClass() {
        readerContext =
                writeClient
                        .getEngineContext()
                        .<RowData>getReaderContextFactory(metaClient)
                        .getContext();
        readerContext.initRecordMergerForIngestion(writeClient.getConfig().getProps());

        recordMerger =
                BufferedRecordMergerFactory.create(
                        readerContext,
                        readerContext.getMergeMode(),
                        false,
                        readerContext.getRecordMerger(),
                        new Schema.Parser().parse(writeClient.getConfig().getSchema()),
                        readerContext.getPayloadClasses(writeClient.getConfig().getProps()),
                        writeClient.getConfig().getProps(),
                        metaClient.getTableConfig().getPartialUpdateMode());
        LOG.info("init hoodie merge with class [{}]", recordMerger.getClass().getName());
    }

    private boolean doBufferRecord(String bucketID, HoodieFlinkInternalRow record)
            throws IOException {
        try {
            RowDataBucket bucket =
                    this.buckets.computeIfAbsent(
                            bucketID,
                            k ->
                                    new RowDataBucket(
                                            bucketID,
                                            BufferUtils.createBuffer(rowType, memorySegmentPool),
                                            getBucketInfo(record),
                                            this.config.get(FlinkOptions.WRITE_BATCH_SIZE)));

            return bucket.writeRow(record.getRowData());
        } catch (MemoryPagesExhaustedException e) {
            LOG.info(
                    "There is no enough free pages in memory pool to create buffer, need flushing first.",
                    e);
            return false;
        }
    }

    /**
     * Buffers the given record.
     *
     * <p>Flush the data bucket first if the bucket records size is greater than the configured
     * value {@link FlinkOptions#WRITE_BATCH_SIZE}.
     *
     * <p>Flush the max size data bucket if the total buffer size exceeds the configured threshold
     * {@link FlinkOptions#WRITE_TASK_MAX_SIZE}.
     *
     * @param record HoodieFlinkInternalRow
     */
    protected void bufferRecord(HoodieFlinkInternalRow record) throws IOException {
        writeMetrics.markRecordIn();
        // set operation type into rowkind of row.
        record.getRowData()
                .setRowKind(
                        RowKind.fromByteValue(
                                HoodieOperation.fromName(record.getOperationType()).getValue()));
        final String bucketID = getBucketID(record.getPartitionPath(), record.getFileId());

        // 1. try buffer the record into the memory pool
        boolean success = doBufferRecord(bucketID, record);
        if (!success) {
            // 2. flushes the bucket if the memory pool is full
            RowDataBucket bucketToFlush =
                    this.buckets.values().stream()
                            .max(Comparator.comparingLong(RowDataBucket::getBufferSize))
                            .orElseThrow(NoSuchElementException::new);
            if (flushBucket(bucketToFlush)) {
                // 2.1 flushes the data bucket with maximum size
                this.tracer.countDown(bucketToFlush.getBufferSize());
                disposeBucket(bucketToFlush.getBucketId());
            } else {
                LOG.warn(
                        "The buffer size hits the threshold {}, but still flush the max size data bucket failed!",
                        this.tracer.maxBufferSize);
            }
            // 2.2 try to write row again
            success = doBufferRecord(bucketID, record);
            if (!success) {
                throw new RuntimeException("Buffer is too small to hold a single record.");
            }
        }
        RowDataBucket bucket = this.buckets.get(bucketID);
        this.tracer.trace(bucket.getLastRecordSize());
        // 3. flushes the bucket if it is full
        if (bucket.isFull()) {
            if (flushBucket(bucket)) {
                this.tracer.countDown(bucket.getBufferSize());
                disposeBucket(bucket.getBucketId());
            }
        }
        // update buffer metrics after tracing buffer size
        writeMetrics.setWriteBufferedSize(this.tracer.bufferSize);
    }

    private void disposeBucket(String bucketID) {
        RowDataBucket bucket = this.buckets.remove(bucketID);
        if (bucket != null) {
            bucket.dispose();
        }
    }

    private String getBucketID(String partitionPath, String fileId) {
        return StreamerUtil.generateBucketKey(partitionPath, fileId);
    }

    private static BucketInfo getBucketInfo(HoodieFlinkInternalRow internalRow) {
        BucketType bucketType;
        switch (internalRow.getInstantTime()) {
            case "I":
                bucketType = BucketType.INSERT;
                break;
            case "U":
                bucketType = BucketType.UPDATE;
                break;
            default:
                throw new HoodieException(
                        "Unexpected bucket type: " + internalRow.getInstantTime());
        }
        return new BucketInfo(bucketType, internalRow.getFileId(), internalRow.getPartitionPath());
    }

    private boolean hasData() {
        return !this.buckets.isEmpty()
                && this.buckets.values().stream().anyMatch(bucket -> !bucket.isEmpty());
    }

    private boolean flushBucket(RowDataBucket bucket) {
        return flushBucket(bucket, this.rowType);
    }

    private boolean flushBucket(RowDataBucket bucket, RowType schemaToUse) {
        String instant = instantToWriteForTable(true);

        if (instant == null) {
            LOG.info("No inflight instant when flushing data, skip.");
            return false;
        }

        ValidationUtils.checkState(
                !bucket.isEmpty(), "Data bucket to flush has no buffering records");
        final List<WriteStatus> writeStatus = writeRecords(instant, bucket, schemaToUse);
        final WriteMetadataEvent event =
                WriteMetadataEvent.builder()
                        .taskID(taskID)
                        .checkpointId(this.checkpointId)
                        .instantTime(instant)
                        .writeStatus(writeStatus)
                        .lastBatch(false)
                        .endInput(false)
                        .build();

        this.eventGateway.sendEventToCoordinator(event);
        writeStatuses.addAll(writeStatus);
        return true;
    }

    protected void flushRemaining(boolean endInput) {
        writeMetrics.startDataFlush();
        this.currentInstant = instantToWriteForTable(hasData());
        if (this.currentInstant == null) {
            if (hasData()) {
                throw new HoodieException("No inflight instant when flushing data!");
            } else {
                LOG.info("No data to flush and no inflight instant, sending empty commit metadata");
                final WriteMetadataEvent event =
                        WriteMetadataEvent.builder()
                                .taskID(taskID)
                                .checkpointId(checkpointId)
                                .instantTime(instantToWrite(false))
                                .writeStatus(Collections.emptyList())
                                .lastBatch(true)
                                .endInput(endInput)
                                .build();
                this.eventGateway.sendEventToCoordinator(event);
                return;
            }
        }
        final List<WriteStatus> writeStatus;
        if (!buckets.isEmpty()) {
            writeStatus = new ArrayList<>();
            // Create a snapshot of bucket IDs to avoid issues with disposed buckets
            List<String> bucketIds = new ArrayList<>(buckets.keySet());
            for (String bucketId : bucketIds) {
                RowDataBucket bucket = buckets.get(bucketId);
                if (bucket != null && !bucket.isEmpty()) {
                    writeStatus.addAll(writeRecords(currentInstant, bucket));
                }
                // Remove and dispose bucket immediately after writing
                disposeBucket(bucketId);
            }
        } else {
            LOG.info("No data to write in subtask [{}] for instant [{}]", taskID, currentInstant);
            writeStatus = Collections.emptyList();
        }
        final WriteMetadataEvent event =
                WriteMetadataEvent.builder()
                        .taskID(taskID)
                        .checkpointId(checkpointId)
                        .instantTime(currentInstant)
                        .writeStatus(writeStatus)
                        .lastBatch(true)
                        .endInput(endInput)
                        .build();

        this.eventGateway.sendEventToCoordinator(event);
        this.buckets.clear();
        this.tracer.reset();
        this.writeClient.cleanHandles();
        this.writeStatuses.addAll(writeStatus);

        writeMetrics.endDataFlush();
        writeMetrics.resetAfterCommit();
    }

    protected List<WriteStatus> writeRecords(String instant, RowDataBucket rowDataBucket) {
        return writeRecords(instant, rowDataBucket, this.rowType);
    }

    protected List<WriteStatus> writeRecords(
            String instant, RowDataBucket rowDataBucket, RowType schemaToUse) {
        writeMetrics.startFileFlush();

        Iterator<BinaryRowData> rowItr =
                new MutableIteratorWrapperIterator<>(
                        rowDataBucket.getDataIterator(),
                        () -> new BinaryRowData(schemaToUse.getFieldCount()));
        Iterator<HoodieRecord> recordItr =
                new MappingIterator<>(
                        rowItr,
                        rowData -> recordConverter.convert(rowData, rowDataBucket.getBucketInfo()));

        List<WriteStatus> statuses =
                writeFunction.write(
                        deduplicateRecordsIfNeeded(recordItr),
                        rowDataBucket.getBucketInfo(),
                        instant);
        writeMetrics.endFileFlush();
        writeMetrics.increaseNumOfFilesWritten();
        return statuses;
    }

    protected Iterator<HoodieRecord> deduplicateRecordsIfNeeded(Iterator<HoodieRecord> records) {
        if (config.get(FlinkOptions.PRE_COMBINE)) {
            return FlinkWriteHelper.newInstance()
                    .deduplicateRecords(
                            records,
                            null,
                            -1,
                            this.writeClient.getConfig().getSchema(),
                            this.writeClient.getConfig().getProps(),
                            recordMerger,
                            readerContext,
                            orderingFieldNames.toArray(new String[0]));
        } else {
            return records;
        }
    }

    /**
     * Table-specific version of instantToWrite that delegates to the parent's instantToWrite
     * method. The table information is passed through the TableAwareCorrespondent that was set by
     * MultiTableEventStreamWriteFunction.
     */
    protected String instantToWriteForTable(boolean hasData) {
        if (!hasData) {
            return null;
        }

        if (tableId == null) {
            throw new IllegalStateException(
                    "TableId must be set before requesting instant from coordinator");
        }

        // Use the parent's instant request mechanism
        // The TableAwareCorrespondent handles sending the table-specific requests
        return instantToWrite(hasData);
    }

    /**
     * Flush all buckets immediately. Called when schema changes to ensure no data with old schema
     * remains in buffers.
     *
     * @param schemaToUse The RowType schema to use for flushing (should be the OLD schema before
     *     the change)
     */
    public void flushAllBuckets(RowType schemaToUse) {
        LOG.info(
                "Flushing all {} buckets with schema containing {} fields due to schema change",
                buckets.size(),
                schemaToUse.getFieldCount());
        if (buckets.isEmpty()) {
            LOG.debug("No buckets to flush");
            return;
        }

        // Create a snapshot of bucket IDs to avoid concurrent modification
        List<String> bucketIds = new ArrayList<>(buckets.keySet());

        // Flush and dispose all buckets using the provided schema
        for (String bucketId : bucketIds) {
            RowDataBucket bucket = buckets.get(bucketId);
            if (bucket != null && !bucket.isEmpty()) {
                try {
                    flushBucket(bucket, schemaToUse);
                } catch (Exception e) {
                    LOG.error("Failed to flush bucket {} during schema change", bucketId, e);
                    // Continue flushing other buckets even if one fails
                }
            }
            // Dispose and remove bucket immediately to prevent access to disposed buckets
            disposeBucket(bucketId);
        }

        tracer.reset();
        LOG.info("All buckets flushed and cleared");
    }

    /**
     * Update the rowType when schema evolves. This ensures new buffers are created with the correct
     * schema. Note: keyGen is not updated since primary keys cannot change during schema evolution.
     *
     * @param newRowType The new RowType after schema evolution
     */
    public void updateRowType(RowType newRowType) {
        LOG.info(
                "Updating RowType from {} fields to {} fields",
                rowType.getFieldCount(),
                newRowType.getFieldCount());
        this.rowType = newRowType;

        // Note: We do NOT call initMergeClass() here because:
        // 1. We just flushed buffered data with OLD schema to parquet files
        // 2. If we reinit merge components now, Hudi will expect NEW schema
        // 3. During the next checkpoint, Hudi may need to read those files for merging
        // 4. Reading old files with new converters may cause IndexOutOfBoundsException
        //
        // The merge components will use the old Avro schema until the next open() or
        // until we explicitly update Hudi's table schema metadata via HudiMetadataApplier

        // Log active timeline state for debugging
        logActiveTimeline();

        LOG.info("RowType updated successfully");
    }

    public void updateWriteClientWithNewSchema(String newAvroSchema) {
        this.config.setString(FlinkOptions.SOURCE_AVRO_SCHEMA.key(), newAvroSchema);
        this.writeClient = FlinkWriteClients.createWriteClient(this.config, getRuntimeContext());
    }

    /** Logs the current state of the active timeline for debugging purposes. */
    private void logActiveTimeline() {
        try {
            if (metaClient != null) {
                metaClient.reloadActiveTimeline();
                HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

                LOG.info("Active timeline state for table {}:", tableId);
                LOG.info(
                        "  - Completed commits: {}",
                        activeTimeline
                                .getCommitsTimeline()
                                .filterCompletedInstants()
                                .countInstants());
                LOG.info(
                        "  - Pending commits: {}",
                        activeTimeline
                                .getCommitsTimeline()
                                .filterPendingExcludingCompaction()
                                .countInstants());

                List<String> instantsInfo = new ArrayList<>();
                activeTimeline
                        .getInstants()
                        .forEach(
                                instant ->
                                        instantsInfo.add(
                                                instant.requestedTime()
                                                        + "("
                                                        + instant.getState()
                                                        + ")"));
                LOG.info("  - All instants: {}", instantsInfo);

                LOG.info(
                        "  - Latest completed commit: {}",
                        activeTimeline
                                .getCommitsTimeline()
                                .filterCompletedInstants()
                                .lastInstant()
                                .map(instant -> instant.requestedTime())
                                .orElse("None"));
            }
        } catch (Exception e) {
            LOG.warn("Failed to log active timeline state", e);
        }
    }

    // metrics are now created per table via getOrCreateWriteMetrics(TableId) when needed

    // -------------------------------------------------------------------------
    //  Getter/Setter
    // -------------------------------------------------------------------------

    @VisibleForTesting
    @SuppressWarnings("rawtypes")
    public Map<String, List<HoodieRecord>> getDataBuffer() {
        Map<String, List<HoodieRecord>> ret = new HashMap<>();
        for (Map.Entry<String, RowDataBucket> entry : buckets.entrySet()) {
            List<HoodieRecord> records = new ArrayList<>();
            Iterator<BinaryRowData> rowItr =
                    new MutableIteratorWrapperIterator<>(
                            entry.getValue().getDataIterator(),
                            () -> new BinaryRowData(rowType.getFieldCount()));
            while (rowItr.hasNext()) {
                records.add(
                        recordConverter.convert(rowItr.next(), entry.getValue().getBucketInfo()));
            }
            ret.put(entry.getKey(), records);
        }
        return ret;
    }

    // -------------------------------------------------------------------------
    //  Inner Classes
    // -------------------------------------------------------------------------

    /** Write function to trigger the actual write action. */
    protected interface WriteFunction extends Serializable {
        List<WriteStatus> write(
                Iterator<HoodieRecord> records, BucketInfo bucketInfo, String instant);
    }
}
