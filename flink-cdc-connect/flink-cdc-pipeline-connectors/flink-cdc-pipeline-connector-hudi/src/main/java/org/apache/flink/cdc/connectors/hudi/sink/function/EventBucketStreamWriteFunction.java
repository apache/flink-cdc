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

package org.apache.flink.cdc.connectors.hudi.sink.function;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.hudi.sink.event.HudiRecordEventSerializer;
import org.apache.flink.cdc.connectors.hudi.sink.event.HudiRecordSerializer;
import org.apache.flink.cdc.connectors.hudi.sink.model.BucketAssignmentIndex;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.index.bucket.partition.NumBucketsFunction;
import org.apache.hudi.utils.RuntimeContextUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EventBucketStreamWriteFunction extends EventStreamWriteFunction {

    private static final Logger LOG = LoggerFactory.getLogger(EventBucketStreamWriteFunction.class);

    private int parallelism;

    private boolean isNonBlockingConcurrencyControl;

    /** BucketID to file group mapping in each partition of a tableId. */
    private BucketAssignmentIndex bucketAssignmentIndex;

    /**
     * Incremental bucket index of the current checkpoint interval, it is needed because the bucket
     * type('I' or 'U') should be decided based on the committed files view, all the records in one
     * bucket should have the same bucket type.
     */
    private Set<String> incBucketIndexes;

    /** Serializer for converting Events to HoodieFlinkInternalRow for single table */
    private HudiRecordEventSerializer recordSerializer;

    /** Function for calculating the task partition to dispatch. */
    private Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc;

    /** Function to calculate num buckets per partition. */
    private NumBucketsFunction numBucketsFunction;

    /** Cached primary key fields for this table */
    private transient List<String> primaryKeyFields;

    /** Cached field getters for primary key fields */
    private transient List<RecordData.FieldGetter> primaryKeyFieldGetters;

    /** Cached schema for this table */
    private transient Schema cachedSchema;

    /** Number of buckets for this function */
    private int numBuckets;

    /**
     * Constructs a BucketStreamWriteFunction.
     *
     * @param config The config options
     */
    public EventBucketStreamWriteFunction(Configuration config, RowType rowType) {
        super(config, rowType);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        super.open(parameters);
        this.isNonBlockingConcurrencyControl =
                OptionsResolver.isNonBlockingConcurrencyControl(config);
        this.taskID = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
        this.parallelism = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
        this.bucketAssignmentIndex = new BucketAssignmentIndex();
        this.incBucketIndexes = new HashSet<>();
        this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(parallelism);
        this.numBucketsFunction =
                new NumBucketsFunction(
                        config.get(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS),
                        config.get(FlinkOptions.BUCKET_INDEX_PARTITION_RULE),
                        config.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS));

        this.numBuckets = config.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);

        // Initialize record serializer with system default zone ID
        this.recordSerializer = new HudiRecordEventSerializer(ZoneId.systemDefault());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);
        // Bootstrap will reload file groups from Hudi on startup
    }

    @Override
    public void snapshotState() {
        LOG.info("Triggering snapshotState");
        super.snapshotState();
        this.incBucketIndexes.clear();
    }

    @Override
    public void processDataChange(DataChangeEvent event) throws Exception {
        // Check if schema is available before processing
        if (!recordSerializer.hasSchema(event.tableId())) {
            // Schema not available yet - CreateTableEvent hasn't arrived
            throw new IllegalStateException(
                    "No schema available for table "
                            + event.tableId()
                            + ". CreateTableEvent should arrive before DataChangeEvent.");
        }

        HoodieFlinkInternalRow hoodieFlinkInternalRow = recordSerializer.serialize(event);
        // Calculate bucket from event data for bucket assignment
        int bucket = calculateBucketFromEvent(event);

        // Define record location (file ID, instant time) based on bucket assignment
        defineRecordLocation(bucket, hoodieFlinkInternalRow);

        // Buffer the record for writing
        bufferRecord(hoodieFlinkInternalRow);

        LOG.debug(
                "Processed DataChangeEvent for table {}: partition={}, fileId={}, instantTime={}",
                event.tableId(),
                hoodieFlinkInternalRow.getPartitionPath(),
                hoodieFlinkInternalRow.getFileId(),
                hoodieFlinkInternalRow.getInstantTime());
    }

    @Override
    public void processSchemaChange(SchemaChangeEvent event) throws Exception {
        // Single-table functions typically receive schema via serializer setup
        // This is called when CreateTableEvent arrives
        LOG.info("Schema change event received: {}", event);

        // Handle schema events (CreateTableEvent, SchemaChangeEvent) - they don't produce records
        // null will be returned from serialize
        recordSerializer.serialize(event);
    }

    private void defineRecordLocation(int bucketNum, HoodieFlinkInternalRow record) {
        final String partition = record.getPartitionPath();

        // Check if this task should handle this bucket
        if (!isBucketToLoad(bucketNum, partition)) {
            throw new IllegalStateException(
                    String.format(
                            "Task %d received record for bucket %d which should not be handled by this task. "
                                    + "This indicates a partitioning problem - records must be routed to the correct task.",
                            taskID, bucketNum));
        }

        bootstrapIndexIfNeed(partition);
        Map<Integer, String> bucketToFileId = bucketAssignmentIndex.getBucketToFileIdMap(partition);
        final String bucketId = partition + "/" + bucketNum;

        if (incBucketIndexes.contains(bucketId)) {
            record.setInstantTime("I");
            record.setFileId(bucketToFileId.get(bucketNum));
        } else if (bucketToFileId.containsKey(bucketNum)) {
            record.setInstantTime("U");
            record.setFileId(bucketToFileId.get(bucketNum));
        } else {
            String newFileId =
                    isNonBlockingConcurrencyControl
                            ? BucketIdentifier.newBucketFileIdForNBCC(bucketNum)
                            : BucketIdentifier.newBucketFileIdPrefix(bucketNum);
            record.setInstantTime("I");
            record.setFileId(newFileId);
            bucketToFileId.put(bucketNum, newFileId);
            incBucketIndexes.add(bucketId);
        }
    }

    /**
     * Determine whether the current fileID belongs to the current task. partitionIndex == this
     * taskID belongs to this task.
     */
    public boolean isBucketToLoad(int bucketNumber, String partition) {
        int numBuckets = numBucketsFunction.getNumBuckets(partition);
        return partitionIndexFunc.apply(numBuckets, partition, bucketNumber) == taskID;
    }

    /**
     * Get partition_bucket -> fileID mapping from the existing hudi table. This is a required
     * operation for each restart to avoid having duplicate file ids for one bucket.
     */
    private void bootstrapIndexIfNeed(String partition) {
        if (bucketAssignmentIndex.containsPartition(partition)) {
            return;
        }
        LOG.info(
                "Loading Hoodie Table {}, with path {}/{}",
                this.metaClient.getTableConfig().getTableName(),
                this.metaClient.getBasePath(),
                partition);

        // Load existing fileID belongs to this task
        Map<Integer, String> bucketToFileIDMap = new HashMap<>();
        this.writeClient
                .getHoodieTable()
                .getHoodieView()
                .getLatestFileSlices(partition)
                .forEach(
                        fileSlice -> {
                            String fileId = fileSlice.getFileId();
                            int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileId);
                            if (isBucketToLoad(bucketNumber, partition)) {
                                LOG.info(
                                        String.format(
                                                "Should load this partition bucket %s with fileId %s",
                                                bucketNumber, fileId));
                                // Validate that one bucketId has only ONE fileId
                                if (bucketToFileIDMap.containsKey(bucketNumber)) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Duplicate fileId %s from bucket %s of partition %s found "
                                                            + "during the BucketStreamWriteFunction index bootstrap.",
                                                    fileId, bucketNumber, partition));
                                } else {
                                    LOG.info(
                                            String.format(
                                                    "Adding fileId %s to the bucket %s of partition %s.",
                                                    fileId, bucketNumber, partition));
                                    bucketToFileIDMap.put(bucketNumber, fileId);
                                }
                            }
                        });
        bucketAssignmentIndex.bootstrapPartition(partition, bucketToFileIDMap);
    }

    /** Calculate bucket from DataChangeEvent using primary key fields. */
    private int calculateBucketFromEvent(DataChangeEvent dataChangeEvent) {
        // Initialize cache on first call
        if (cachedSchema == null) {
            cachedSchema = recordSerializer.getSchema(dataChangeEvent.tableId());
            if (cachedSchema == null) {
                throw new IllegalStateException(
                        "No schema available for table " + dataChangeEvent.tableId());
            }

            // Cache primary key fields
            primaryKeyFields = cachedSchema.primaryKeys();
            if (primaryKeyFields.isEmpty()) {
                throw new IllegalStateException(
                        "Cannot calculate bucket: table "
                                + dataChangeEvent.tableId()
                                + " has no primary keys");
            }

            // Cache field getters for primary key fields
            primaryKeyFieldGetters = new ArrayList<>(primaryKeyFields.size());
            for (String primaryKeyField : primaryKeyFields) {
                int fieldIndex = cachedSchema.getColumnNames().indexOf(primaryKeyField);
                if (fieldIndex == -1) {
                    throw new IllegalStateException(
                            "Primary key field '"
                                    + primaryKeyField
                                    + "' not found in schema for table "
                                    + dataChangeEvent.tableId());
                }
                DataType fieldType = cachedSchema.getColumns().get(fieldIndex).getType();
                primaryKeyFieldGetters.add(RecordData.createFieldGetter(fieldType, fieldIndex));
            }
        }

        // Extract record key from event data using cached field getters
        String recordKey = extractRecordKeyFromEvent(dataChangeEvent);

        // Calculate bucket using Hudi's bucket logic
        return calculateBucketFromRecordKey(recordKey, primaryKeyFields);
    }

    /**
     * Extract record key from CDC event data using cached field getters for optimal performance.
     */
    private String extractRecordKeyFromEvent(DataChangeEvent dataChangeEvent) {
        // For DELETE operations, use 'before' data; for INSERT/UPDATE, use 'after' data
        RecordData recordData =
                dataChangeEvent.op() == OperationType.DELETE
                        ? dataChangeEvent.before()
                        : dataChangeEvent.after();

        if (recordData == null) {
            throw new IllegalStateException(
                    "Cannot extract record key: " + dataChangeEvent.op() + " event has null data");
        }

        // Use cached field getters for optimal performance
        List<String> recordKeyPairs = new ArrayList<>(primaryKeyFields.size());
        for (int i = 0; i < primaryKeyFields.size(); i++) {
            RecordData.FieldGetter fieldGetter = primaryKeyFieldGetters.get(i);
            Object fieldValue = fieldGetter.getFieldOrNull(recordData);

            if (fieldValue == null) {
                throw new IllegalStateException(
                        "Primary key field '" + primaryKeyFields.get(i) + "' is null in record");
            }

            // Format as "fieldName:value"
            recordKeyPairs.add(primaryKeyFields.get(i) + ":" + fieldValue);
        }

        // Join primary key pairs with comma (recordKey1:val1,recordKey2:val2)
        return String.join(",", recordKeyPairs);
    }

    /** Calculate bucket ID from record key using Hudi's bucket logic. */
    private int calculateBucketFromRecordKey(String recordKey, List<String> primaryKeyFields) {
        // Convert primary key field list to comma-separated string for Hudi bucket calculation
        String tableIndexKeyFields = String.join(",", primaryKeyFields);
        return BucketIdentifier.getBucketId(recordKey, tableIndexKeyFields, numBuckets);
    }

    /** Get the record serializer for schema setup. */
    public HudiRecordSerializer<Event> getRecordSerializer() {
        return recordSerializer;
    }
}
