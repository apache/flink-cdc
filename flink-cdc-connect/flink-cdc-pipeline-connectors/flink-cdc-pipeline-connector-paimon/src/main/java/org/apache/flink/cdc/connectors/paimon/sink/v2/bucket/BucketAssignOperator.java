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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.paimon.sink.v2.OperatorIDGenerator;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonWriterHelper;
import org.apache.flink.cdc.connectors.paimon.sink.v2.TableSchemaInfo;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
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

    private Map<TableId, MixedSchemaInfo> schemaMaps;

    private int totalTasksNumber;

    private int currentTaskNumber;

    public final String schemaOperatorUid;

    private transient SchemaEvolutionClient schemaEvolutionClient;

    private final ZoneId zoneId;

    protected SchemaDerivator schemaDerivator;

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
        open(getRuntimeContext().getTaskInfo());
    }

    @VisibleForTesting
    public void open(TaskInfo taskInfo) {
        this.catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        this.bucketAssignerMap = new HashMap<>();
        this.totalTasksNumber = taskInfo.getNumberOfParallelSubtasks();
        this.currentTaskNumber = taskInfo.getIndexOfThisSubtask();
        this.schemaMaps = new HashMap<>();
        this.schemaDerivator = new SchemaDerivator();
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

    @VisibleForTesting
    public void setSchemaEvolutionClient(SchemaEvolutionClient schemaEvolutionClient) {
        this.schemaEvolutionClient = schemaEvolutionClient;
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
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = convertDataChangeEvent((DataChangeEvent) event);
            Tuple4<BucketMode, RowKeyExtractor, BucketAssigner, RowPartitionKeyExtractor> tuple4 =
                    bucketAssignerMap.computeIfAbsent(
                            dataChangeEvent.tableId(), this::getTableInfo);
            int bucket;
            GenericRow genericRow =
                    PaimonWriterHelper.convertEventToGenericRow(
                            dataChangeEvent,
                            schemaMaps
                                    .get(dataChangeEvent.tableId())
                                    .getPaimonSchemaInfo()
                                    .getFieldGetters());
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
                    new StreamRecord<>(new BucketWrapperChangeEvent(bucket, dataChangeEvent)));
        } else {
            // Broadcast SchemachangeEvent.
            for (int index = 0; index < totalTasksNumber; index++) {
                output.collect(
                        new StreamRecord<>(
                                new BucketWrapperChangeEvent(
                                        index,
                                        convertSchemaChangeEvent((SchemaChangeEvent) event))));
            }
        }
    }

    @VisibleForTesting
    public SchemaChangeEvent convertSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent)
            throws Exception {
        if (schemaChangeEvent instanceof DropTableEvent
                || schemaChangeEvent instanceof TruncateTableEvent) {
            return schemaChangeEvent;
        }
        TableId tableId = schemaChangeEvent.tableId();
        Schema upstreamSchema;
        try {
            upstreamSchema =
                    schemaMaps.containsKey(tableId)
                            ? schemaMaps.get(tableId).getUpstreamSchemaInfo().getSchema()
                            : schemaEvolutionClient.getLatestEvolvedSchema(tableId).orElse(null);
        } catch (Exception e) {
            // In batch mode, we can't get schema from registry.
            upstreamSchema = null;
        }
        if (!SchemaUtils.isSchemaChangeEventRedundant(upstreamSchema, schemaChangeEvent)) {
            upstreamSchema = SchemaUtils.applySchemaChangeEvent(upstreamSchema, schemaChangeEvent);
        }
        Schema physicalSchema =
                PaimonWriterHelper.deduceSchemaForPaimonTable(
                        catalog.getTable(PaimonWriterHelper.identifierFromTableId(tableId)));
        MixedSchemaInfo mixedSchemaInfo =
                new MixedSchemaInfo(
                        new TableSchemaInfo(upstreamSchema, zoneId),
                        new TableSchemaInfo(physicalSchema, zoneId));
        if (!mixedSchemaInfo.isSameColumnsIgnoringCommentAndDefaultValue()) {
            LOGGER.warn(
                    "Upstream schema of {} is {}, which is different with paimon physical table schema {}. Data precision loss and truncation may occur.",
                    tableId,
                    upstreamSchema,
                    physicalSchema);
        }
        schemaMaps.put(tableId, mixedSchemaInfo);
        return new CreateTableEvent(tableId, physicalSchema);
    }

    @VisibleForTesting
    public DataChangeEvent convertDataChangeEvent(DataChangeEvent dataChangeEvent)
            throws Exception {
        TableId tableId = dataChangeEvent.tableId();
        if (!schemaMaps.containsKey(dataChangeEvent.tableId())) {
            Optional<Schema> schema;
            try {
                schema = schemaEvolutionClient.getLatestEvolvedSchema(dataChangeEvent.tableId());
            } catch (Exception e) {
                // In batch mode, we can't get schema from registry.
                schema = Optional.empty();
            }
            if (schema.isPresent()) {
                MixedSchemaInfo mixedSchemaInfo =
                        new MixedSchemaInfo(
                                new TableSchemaInfo(schema.get(), zoneId),
                                new TableSchemaInfo(
                                        PaimonWriterHelper.deduceSchemaForPaimonTable(
                                                catalog.getTable(
                                                        PaimonWriterHelper.identifierFromTableId(
                                                                tableId))),
                                        zoneId));
                if (!mixedSchemaInfo.isSameColumnsIgnoringCommentAndDefaultValue()) {
                    LOGGER.warn(
                            "Upstream schema of {} is {}, which is different with paimon physical table schema {}. Data precision loss and truncation may occur.",
                            tableId,
                            mixedSchemaInfo.getUpstreamSchemaInfo().getSchema(),
                            mixedSchemaInfo.getPaimonSchemaInfo().getSchema());
                }
                // Broadcast the CreateTableEvent with physical schema after job restarted.
                // This is necessary because the DataSinkOperator would emit the upstream schema.
                for (int index = 0; index < totalTasksNumber; index++) {
                    output.collect(
                            new StreamRecord<>(
                                    new BucketWrapperChangeEvent(
                                            index,
                                            new CreateTableEvent(
                                                    tableId,
                                                    mixedSchemaInfo.paimonSchemaInfo
                                                            .getSchema()))));
                }
                schemaMaps.put(tableId, mixedSchemaInfo);
            } else {
                throw new RuntimeException(
                        "Could not find schema message from SchemaRegistry for "
                                + tableId
                                + ", this may because of this table was dropped.");
            }
        }
        MixedSchemaInfo mixedSchemaInfo = schemaMaps.get(tableId);
        if (!mixedSchemaInfo.isSameColumnsIgnoringCommentAndDefaultValue()) {
            dataChangeEvent =
                    schemaDerivator
                            .coerceDataRecord(
                                    zoneId.getId(),
                                    dataChangeEvent,
                                    mixedSchemaInfo.getUpstreamSchemaInfo().getSchema(),
                                    mixedSchemaInfo.getPaimonSchemaInfo().getSchema())
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    String.format(
                                                            "Unable to coerce data record of %s from (schema: %s) to (schema: %s)",
                                                            tableId,
                                                            mixedSchemaInfo
                                                                    .getUpstreamSchemaInfo()
                                                                    .getSchema(),
                                                            mixedSchemaInfo
                                                                    .getPaimonSchemaInfo()
                                                                    .getSchema())));
        }
        return dataChangeEvent;
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
        Integer maxBucketsNum = table.coreOptions().dynamicBucketMaxBuckets();
        LOGGER.debug("Successfully get table info {}", table);
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
                        targetRowNum,
                        maxBucketsNum),
                new RowPartitionKeyExtractor(table.schema()));
    }

    /** MixedSchemaInfo is used to store the mixed schema info of upstream and paimon table. */
    private static class MixedSchemaInfo {
        private final TableSchemaInfo upstreamSchemaInfo;

        private final TableSchemaInfo paimonSchemaInfo;

        private final boolean sameColumnsIgnoringCommentAndDefaultValue;

        public MixedSchemaInfo(
                TableSchemaInfo upstreamSchemaInfo, TableSchemaInfo paimonSchemaInfo) {
            this.upstreamSchemaInfo = upstreamSchemaInfo;
            this.paimonSchemaInfo = paimonSchemaInfo;
            this.sameColumnsIgnoringCommentAndDefaultValue =
                    PaimonWriterHelper.sameColumnsIgnoreCommentAndDefaultValue(
                            upstreamSchemaInfo.getSchema(), paimonSchemaInfo.getSchema());
        }

        public TableSchemaInfo getUpstreamSchemaInfo() {
            return upstreamSchemaInfo;
        }

        public TableSchemaInfo getPaimonSchemaInfo() {
            return paimonSchemaInfo;
        }

        public boolean isSameColumnsIgnoringCommentAndDefaultValue() {
            return sameColumnsIgnoringCommentAndDefaultValue;
        }
    }
}
