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

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;
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

/** Extension of EventStreamWriteFunction to handle bucketing. */
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

    /** Serializer for converting Events to HoodieFlinkInternalRow for single table. */
    private HudiRecordEventSerializer recordSerializer;

    /** Function for calculating which task should handle a given bucket. */
    private Functions.Function3<Integer, String, Integer, Integer> taskAssignmentFunc;

    /** Function to calculate num buckets per partition. */
    private NumBucketsFunction numBucketsFunction;

    /** Cached primary key fields for this table. */
    private transient List<String> primaryKeyFields;

    /** Number of buckets for this function. */
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
        this.taskAssignmentFunc = BucketIndexUtil.getPartitionIndexFunc(parallelism);
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
        // Calculate bucket from the serialized Hudi record
        int bucket = calculateBucketFromRecord(hoodieFlinkInternalRow);

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

        // Cache the schema's primary keys for bucket calculation
        Schema schema = recordSerializer.getSchema(event.tableId());
        if (schema != null) {
            primaryKeyFields = schema.primaryKeys();
            if (primaryKeyFields == null || primaryKeyFields.isEmpty()) {
                throw new IllegalStateException(
                        "Cannot initialize bucket calculation: table "
                                + event.tableId()
                                + " has no primary keys");
            }
        }
    }

    private void defineRecordLocation(int bucketId, HoodieFlinkInternalRow record) {
        final String partition = record.getPartitionPath();

        // Check if this task should handle this bucket
        if (!shouldTaskHandleBucket(bucketId, partition)) {
            throw new IllegalStateException(
                    String.format(
                            "Task %d received record for bucket %d which should not be handled by this task. "
                                    + "This indicates a partitioning problem - records must be routed to the correct task.",
                            taskID, bucketId));
        }

        bootstrapIndexIfNeed(partition);
        Map<Integer, String> bucketToFileId = bucketAssignmentIndex.getBucketToFileIdMap(partition);
        final String bucketKey = partition + "/" + bucketId;

        if (incBucketIndexes.contains(bucketKey)) {
            record.setInstantTime("I");
            record.setFileId(bucketToFileId.get(bucketId));
        } else if (bucketToFileId.containsKey(bucketId)) {
            record.setInstantTime("U");
            record.setFileId(bucketToFileId.get(bucketId));
        } else {
            String newFileId =
                    isNonBlockingConcurrencyControl
                            ? BucketIdentifier.newBucketFileIdForNBCC(bucketId)
                            : BucketIdentifier.newBucketFileIdPrefix(bucketId);
            record.setInstantTime("I");
            record.setFileId(newFileId);
            bucketToFileId.put(bucketId, newFileId);
            incBucketIndexes.add(bucketKey);
        }
    }

    /**
     * Determine whether this task should handle the given bucket. Returns true if the bucket is
     * assigned to this task based on the task assignment function.
     */
    public boolean shouldTaskHandleBucket(int bucketNumber, String partition) {
        int numBuckets = numBucketsFunction.getNumBuckets(partition);
        return taskAssignmentFunc.apply(numBuckets, partition, bucketNumber) == taskID;
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
                            if (shouldTaskHandleBucket(bucketNumber, partition)) {
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

    /**
     * Calculate bucket from HoodieFlinkInternalRow using the record key. The record key is already
     * computed by the serializer during conversion from CDC event.
     */
    private int calculateBucketFromRecord(HoodieFlinkInternalRow record) {
        // Get record key directly from HoodieFlinkInternalRow
        String recordKey = record.getRecordKey();

        // Initialize primary key fields lazily if not already done
        if (primaryKeyFields == null) {
            // Parse the record key to extract field names
            // Record key format: "fieldName1:value1,fieldName2:value2"
            String[] keyPairs = recordKey.split(",");
            primaryKeyFields = new ArrayList<>(keyPairs.length);
            for (String keyPair : keyPairs) {
                String[] parts = keyPair.split(":", 2);
                if (parts.length == 2) {
                    primaryKeyFields.add(parts[0]);
                } else {
                    throw new IllegalStateException(
                            "Invalid record key format: "
                                    + recordKey
                                    + ". Expected 'fieldName:value' pairs.");
                }
            }
        }

        // Convert primary key field list to comma-separated string for Hudi bucket calculation
        String tableIndexKeyFields = String.join(",", primaryKeyFields);
        return BucketIdentifier.getBucketId(recordKey, tableIndexKeyFields, numBuckets);
    }

    /** Get the record serializer for schema setup. */
    public HudiRecordSerializer<Event> getRecordSerializer() {
        return recordSerializer;
    }
}
