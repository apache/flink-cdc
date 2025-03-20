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

package org.apache.flink.cdc.connectors.paimon.sink.v2.bucket;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.paimon.sink.v2.OperatorIDGenerator;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonWriterHelper;
import org.apache.flink.cdc.connectors.paimon.sink.v2.TableSchemaInfo;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.utils.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Assign bucket for every given {@link DataChangeEvent}. */
public class BucketAssignOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BucketAssignOperator.class);

    public final String commitUser;

    private final Options catalogOptions;

    private Catalog catalog;

    Map<TableId, Tuple4<BucketMode, RowKeyExtractor, BucketAssigner, RowPartitionKeyExtractor>>
            bucketAssignerMap;

    // maintain the latest schema of tableId.
    private Map<TableId, TableSchemaInfo> schemaMaps;

    private int totalTasksNumber;

    private int currentTaskNumber;

    public final String schemaOperatorUid;

    private transient SchemaEvolutionClient schemaEvolutionClient;

    private final ZoneId zoneId;

    public BucketAssignOperator(
            Options catalogOptions, String schemaOperatorUid, ZoneId zoneId, String commitUser) {
        this.catalogOptions = catalogOptions;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.schemaOperatorUid = schemaOperatorUid;
        this.commitUser = commitUser;
        this.zoneId = zoneId;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        this.bucketAssignerMap = new HashMap<>();
        this.totalTasksNumber = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        this.currentTaskNumber = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        this.schemaMaps = new HashMap<>();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        toCoordinator, new OperatorIDGenerator(schemaOperatorUid).generate());
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) throws Exception {
        Event event = streamRecord.getValue();
        if (event instanceof FlushEvent) {
            for (int i = 0; i < totalTasksNumber; i++) {
                output.collect(
                        new StreamRecord<>(
                                new BucketWrapperFlushEvent(
                                        i,
                                        ((FlushEvent) event).getSourceSubTaskId(),
                                        currentTaskNumber,
                                        ((FlushEvent) event).getTableIds(),
                                        ((FlushEvent) event).getSchemaChangeEventType())));
            }
            return;
        }

        if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            if (!schemaMaps.containsKey(dataChangeEvent.tableId())) {
                Optional<Schema> schema =
                        schemaEvolutionClient.getLatestEvolvedSchema(dataChangeEvent.tableId());
                if (schema.isPresent()) {
                    schemaMaps.put(
                            dataChangeEvent.tableId(), new TableSchemaInfo(schema.get(), zoneId));
                } else {
                    throw new RuntimeException(
                            "Could not find schema message from SchemaRegistry for "
                                    + dataChangeEvent.tableId());
                }
            }
            Tuple4<BucketMode, RowKeyExtractor, BucketAssigner, RowPartitionKeyExtractor> tuple4 =
                    bucketAssignerMap.computeIfAbsent(
                            dataChangeEvent.tableId(), this::getTableInfo);
            int bucket;
            GenericRow genericRow =
                    PaimonWriterHelper.convertEventToGenericRow(
                            dataChangeEvent,
                            schemaMaps.get(dataChangeEvent.tableId()).getFieldGetters());
            switch (tuple4.f0) {
                case HASH_DYNAMIC:
                    {
                        bucket =
                                tuple4.f2.assign(
                                        tuple4.f3.partition(genericRow),
                                        tuple4.f3.trimmedPrimaryKey(genericRow).hashCode());
                        break;
                    }
                case HASH_FIXED:
                    {
                        tuple4.f1.setRecord(genericRow);
                        bucket = tuple4.f1.bucket();
                        break;
                    }
                case BUCKET_UNAWARE:
                    {
                        bucket = 0;
                        break;
                    }
                case CROSS_PARTITION:
                default:
                    {
                        throw new RuntimeException("Unsupported bucket mode: " + tuple4.f0);
                    }
            }
            output.collect(
                    new StreamRecord<>(new BucketWrapperChangeEvent(bucket, (ChangeEvent) event)));
        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            Schema schema =
                    SchemaUtils.applySchemaChangeEvent(
                            Optional.ofNullable(schemaMaps.get(schemaChangeEvent.tableId()))
                                    .map(TableSchemaInfo::getSchema)
                                    .orElse(null),
                            schemaChangeEvent);
            schemaMaps.put(schemaChangeEvent.tableId(), new TableSchemaInfo(schema, zoneId));
            // Broadcast SchemachangeEvent.
            for (int index = 0; index < totalTasksNumber; index++) {
                output.collect(
                        new StreamRecord<>(
                                new BucketWrapperChangeEvent(index, (ChangeEvent) event)));
            }
        }
    }

    private Tuple4<BucketMode, RowKeyExtractor, BucketAssigner, RowPartitionKeyExtractor>
            getTableInfo(TableId tableId) {
        Preconditions.checkNotNull(tableId, "Invalid tableId in given event.");
        FileStoreTable table;
        try {
            table = (FileStoreTable) catalog.getTable(Identifier.fromString(tableId.toString()));
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
        long targetRowNum = table.coreOptions().dynamicBucketTargetRowNum();
        Integer numAssigners = table.coreOptions().dynamicBucketInitialBuckets();
        return new Tuple4<>(
                table.bucketMode(),
                table.createRowKeyExtractor(),
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        table.store().newIndexFileHandler(),
                        totalTasksNumber,
                        MathUtils.min(numAssigners, totalTasksNumber),
                        currentTaskNumber,
                        targetRowNum),
                new RowPartitionKeyExtractor(table.schema()));
    }
}
