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

package org.apache.flink.cdc.connectors.hudi.sink.bucket;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.hudi.sink.util.RowDataUtils;
import org.apache.flink.cdc.connectors.hudi.sink.v2.OperatorIDGenerator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;

import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Operator that assigns bucket indices to events and wraps them for downstream partitioning.
 *
 * <p>This operator:
 *
 * <ul>
 *   <li>Broadcasts schema events (CreateTableEvent, SchemaChangeEvent, FlushEvent) to all
 *       downstream tasks
 *   <li>Calculates bucket for DataChangeEvents and routes to specific task
 *   <li>Wraps events in BucketWrapper for downstream partitioning
 * </ul>
 */
public class BucketAssignOperator extends AbstractStreamOperator<BucketWrapper>
        implements OneInputStreamOperator<Event, BucketWrapper> {

    private static final Logger LOG = LoggerFactory.getLogger(BucketAssignOperator.class);

    private final int numBuckets;
    private final String schemaOperatorUid;
    private int totalTasksNumber;
    private int currentTaskNumber;

    /** Function for calculating the task partition to dispatch based on bucket. */
    private transient Functions.Function3<Integer, String, Integer, Integer> partitionIndexFunc;

    /** Schema evolution client to query schemas from SchemaOperator coordinator. */
    private transient SchemaEvolutionClient schemaEvolutionClient;

    /** Cache of schemas per table for bucket calculation. */
    private final Map<TableId, Schema> schemaCache = new HashMap<>();

    /** RowDataKeyGen cache per table for key and partition extraction. */
    private final Map<TableId, RowDataKeyGen> keyGenCache = new HashMap<>();

    /** Field getter cache per table - lazily created and invalidated on schema changes. */
    private final Map<TableId, List<RecordData.FieldGetter>> fieldGetterCache = new HashMap<>();

    public BucketAssignOperator(Configuration conf, String schemaOperatorUid) {
        this.numBuckets = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
        this.schemaOperatorUid = schemaOperatorUid;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<BucketWrapper>> output) {
        super.setup(containingTask, config, output);
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        toCoordinator, new OperatorIDGenerator(schemaOperatorUid).generate());
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.totalTasksNumber = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        this.currentTaskNumber = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(totalTasksNumber);
        LOG.info(
                "BucketAssignOperator opened with {} buckets and {} tasks",
                numBuckets,
                totalTasksNumber);
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) throws Exception {
        Event event = streamRecord.getValue();

        // Broadcast SchemaChangeEvent (includes CreateTableEvent) to all tasks
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaEvent = (SchemaChangeEvent) event;
            Schema existingSchema = schemaCache.get(schemaEvent.tableId());
            Schema newSchema = SchemaUtils.applySchemaChangeEvent(existingSchema, schemaEvent);
            schemaCache.put(schemaEvent.tableId(), newSchema);

            // Clear caches when schema changes
            keyGenCache.remove(schemaEvent.tableId());
            fieldGetterCache.remove(schemaEvent.tableId());

            // Broadcast to all tasks
            for (int i = 0; i < totalTasksNumber; i++) {
                output.collect(new StreamRecord<>(new BucketWrapper(i, event)));
            }
            return;
        }

        // Broadcast FlushEvent to all tasks wrapped with task metadata
        if (event instanceof FlushEvent) {
            FlushEvent flushEvent = (FlushEvent) event;
            for (int i = 0; i < totalTasksNumber; i++) {
                output.collect(
                        new StreamRecord<>(
                                new BucketWrapper(
                                        i,
                                        new BucketWrapperFlushEvent(
                                                i,
                                                flushEvent.getSourceSubTaskId(),
                                                currentTaskNumber,
                                                flushEvent.getTableIds(),
                                                flushEvent.getSchemaChangeEventType()))));
            }
            return;
        }

        // Calculate bucket for DataChangeEvent and route to specific task
        if (event instanceof DataChangeEvent) {
            DataChangeEvent dataEvent = (DataChangeEvent) event;
            int taskIndex = calculateTaskIndex(dataEvent);
            output.collect(new StreamRecord<>(new BucketWrapper(taskIndex, event)));
            return;
        }

        // Default: broadcast unknown event types to all tasks
        for (int i = 0; i < totalTasksNumber; i++) {
            output.collect(new StreamRecord<>(new BucketWrapper(i, event)));
        }
    }

    /**
     * Calculate which task index should handle this event by: 1. Calculating the bucket number (0
     * to numBuckets-1) based on record key 2. Using partitionIndexFunc to map bucket -> task index
     * for balanced distribution
     *
     * @param event The DataChangeEvent to calculate task index for
     * @return The task index (0 to parallelism-1) that should handle this event
     */
    private int calculateTaskIndex(DataChangeEvent event) {
        TableId tableId = event.tableId();

        // Get or cache schema, query from SchemaOperator coordinator if not cached
        Schema schema = schemaCache.get(tableId);
        if (schema == null) {
            try {
                Optional<Schema> optSchema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
                if (optSchema.isPresent()) {
                    schema = optSchema.get();
                    schemaCache.put(tableId, schema);
                } else {
                    throw new IllegalStateException(
                            "No schema available for table "
                                    + tableId
                                    + " in bucket assignment. "
                                    + "Could not find schema from SchemaOperator coordinator.");
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to retrieve schema for table " + tableId + " from SchemaOperator",
                        e);
            }
        }

        // Create final reference for use in lambda
        final Schema finalSchema = schema;

        // Get or cache primary keys
        List<String> primaryKeys = finalSchema.primaryKeys();

        if (primaryKeys.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot calculate bucket: table " + tableId + " has no primary keys");
        }

        // Get or create RowDataKeyGen for this table
        RowDataKeyGen keyGen =
                keyGenCache.computeIfAbsent(tableId, k -> RowDataUtils.createKeyGen(finalSchema));

        // Get or create field getters for this table, lazily cached
        List<RecordData.FieldGetter> fieldGetters =
                fieldGetterCache.computeIfAbsent(
                        tableId,
                        k -> RowDataUtils.createFieldGetters(finalSchema, ZoneId.systemDefault()));

        // Convert DataChangeEvent to RowData for key extraction
        RowData rowData = RowDataUtils.convertDataChangeEventToRowData(event, fieldGetters);

        // Use RowDataKeyGen to extract record key and partition path
        String recordKey = keyGen.getRecordKey(rowData);
        String partition = keyGen.getPartitionPath(rowData);

        // Calculate bucket using Hudi's logic (0 to numBuckets-1)
        String tableIndexKeyFields = String.join(",", primaryKeys);
        int bucketNumber = BucketIdentifier.getBucketId(recordKey, tableIndexKeyFields, numBuckets);

        // partitionIndexFunc is designed for single table, events may come from different tables,
        // prefix them with tableId e.g. tableId + "_" + partition
        // Use partition function to map bucket to task index for balanced distribution
        return partitionIndexFunc.apply(numBuckets, tableId + "_" + partition, bucketNumber);
    }
}
