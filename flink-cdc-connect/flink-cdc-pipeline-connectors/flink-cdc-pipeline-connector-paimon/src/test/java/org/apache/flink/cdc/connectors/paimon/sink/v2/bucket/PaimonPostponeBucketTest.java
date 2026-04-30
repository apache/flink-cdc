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
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonMetadataApplier;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordEventSerializer;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordSerializer;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.cdc.common.types.DataTypes.STRING;

/** Tests for postpone bucket mode in Paimon bucket assignment. */
class PaimonPostponeBucketTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private static final String TEST_DATABASE = "test";
    private static final TableId TABLE_ID = TableId.tableId(TEST_DATABASE, "table1");

    @Test
    void testBucketAssignOperatorUsesCurrentSubtaskForPostponeBucketTable() throws Exception {
        Options catalogOptions = createCatalogOptions();
        Schema schema = createPostponeBucketSchema();
        new PaimonMetadataApplier(catalogOptions)
                .applySchemaChange(new CreateTableEvent(TABLE_ID, schema));

        BucketAssignOperator bucketAssignOperator =
                new BucketAssignOperator(catalogOptions, null, ZoneId.systemDefault(), null);
        CollectingOutput output = new CollectingOutput();
        setField(bucketAssignOperator, "output", output);
        int currentSubtask = 2;
        bucketAssignOperator.open(createTaskInfo(4, currentSubtask));
        bucketAssignOperator.convertSchemaChangeEvent(new CreateTableEvent(TABLE_ID, schema));

        bucketAssignOperator.processElement(new StreamRecord<>(createInsertEvent("1", "Alice")));

        Assertions.assertThat(output.records).hasSize(1);
        Event event = output.records.get(0).getValue();
        Assertions.assertThat(event).isInstanceOf(BucketWrapperChangeEvent.class);
        BucketWrapperChangeEvent bucketWrapperChangeEvent = (BucketWrapperChangeEvent) event;
        Assertions.assertThat(bucketWrapperChangeEvent.getBucket()).isEqualTo(currentSubtask);
        Assertions.assertThat(bucketWrapperChangeEvent.getBucket())
                .isNotEqualTo(BucketMode.POSTPONE_BUCKET);
        Assertions.assertThat(bucketWrapperChangeEvent.getInnerEvent())
                .isInstanceOf(DataChangeEvent.class);
    }

    @Test
    void testSerializerKeepsAssignedBucketBeforeWriterRewrite() throws Exception {
        Schema schema = createPostponeBucketSchema();
        PaimonRecordSerializer<Event> serializer =
                new PaimonRecordEventSerializer(ZoneId.systemDefault());
        serializer.serialize(new CreateTableEvent(TABLE_ID, schema));
        int assignedBucket = 2;
        BucketWrapperChangeEvent bucketWrapperChangeEvent =
                new BucketWrapperChangeEvent(assignedBucket, 0, createInsertEvent("1", "Alice"));

        Assertions.assertThat(serializer.serialize(bucketWrapperChangeEvent).getBucket())
                .isEqualTo(assignedBucket);
    }

    @Test
    void testPostponeBucketTableRewritesAssignedBucketToMinusTwoWhenWriting() throws Exception {
        Options catalogOptions = createCatalogOptions();
        Schema schema = createPostponeBucketSchema();
        new PaimonMetadataApplier(catalogOptions)
                .applySchemaChange(new CreateTableEvent(TABLE_ID, schema));
        FileStoreTable table =
                (FileStoreTable)
                        FlinkCatalogFactory.createPaimonCatalog(catalogOptions)
                                .getTable(
                                        org.apache.paimon.catalog.Identifier.fromString(
                                                TABLE_ID.toString()));

        int assignedBucket = 2;
        List<Integer> writtenBuckets = new ArrayList<>();
        try (MockedConstruction<org.apache.flink.cdc.connectors.paimon.sink.v2.StoreSinkWriteImpl>
                ignored =
                        Mockito.mockConstruction(
                                org.apache.flink.cdc.connectors.paimon.sink.v2.StoreSinkWriteImpl
                                        .class,
                                (mock, context) -> {
                                    Mockito.doAnswer(
                                                    invocation -> {
                                                        writtenBuckets.add(
                                                                invocation.getArgument(1));
                                                        return null;
                                                    })
                                            .when(mock)
                                            .write(Mockito.any(GenericRow.class), Mockito.anyInt());
                                })) {
            org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonWriter<Event> writer =
                    new org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonWriter<>(
                            catalogOptions,
                            UnregisteredMetricsGroup.createSinkWriterMetricGroup(),
                            "test-user",
                            new PaimonRecordEventSerializer(ZoneId.systemDefault()),
                            0);

            writer.write(new CreateTableEvent(TABLE_ID, schema), null);
            writer.write(
                    new BucketWrapperChangeEvent(
                            assignedBucket, 0, createInsertEvent("1", "Alice")),
                    null);
            writer.close();
        }

        Assertions.assertThat(table.bucketMode()).isEqualTo(BucketMode.POSTPONE_MODE);
        Assertions.assertThat(writtenBuckets).containsExactly(BucketMode.POSTPONE_BUCKET);
    }

    private Options createCatalogOptions() {
        Options catalogOptions = new Options();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        return catalogOptions;
    }

    private Schema createPostponeBucketSchema() {
        return Schema.newBuilder()
                .physicalColumn("col1", STRING().notNull())
                .physicalColumn("col2", STRING())
                .primaryKey("col1")
                .option("bucket", String.valueOf(BucketMode.POSTPONE_BUCKET))
                .build();
    }

    private DataChangeEvent createInsertEvent(String col1, String col2) {
        return DataChangeEvent.insertEvent(
                TABLE_ID,
                generate(Arrays.asList(Tuple2.of(STRING(), col1), Tuple2.of(STRING(), col2))));
    }

    private BinaryRecordData generate(List<Tuple2<DataType, Object>> elements) {
        org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator generator =
                new org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator(
                        RowType.of(elements.stream().map(e -> e.f0).toArray(DataType[]::new)));
        return generator.generate(
                elements.stream()
                        .map(e -> e.f1)
                        .map(o -> o instanceof String ? BinaryStringData.fromString((String) o) : o)
                        .toArray(Object[]::new));
    }

    private TaskInfo createTaskInfo(int numberOfParallelSubtasks, int indexOfThisSubtask) {
        return new TaskInfoImpl(
                "test-task",
                numberOfParallelSubtasks,
                indexOfThisSubtask,
                numberOfParallelSubtasks,
                0);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> current = target.getClass();
        while (current != null) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static class CollectingOutput implements Output<StreamRecord<Event>> {
        private final List<StreamRecord<Event>> records = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> streamRecord) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void collect(StreamRecord<Event> record) {
            records.add(record);
        }

        @Override
        public void close() {}
    }
}
