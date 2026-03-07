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

package org.apache.flink.cdc.composer.testsource.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Distributed source using FLIP-27 Source API for testing purposes only. */
public class DistributedSource
        implements Source<
                Event,
                DistributedSource.DistributedSourceSplit,
                Collection<DistributedSource.DistributedSourceSplit>> {

    private static final long serialVersionUID = 1L;

    private final int numOfTables;
    private final boolean distributedTables;

    public DistributedSource(int numOfTables, boolean distributedTables) {
        this.numOfTables = numOfTables;
        this.distributedTables = distributedTables;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<DistributedSourceSplit, Collection<DistributedSourceSplit>>
            createEnumerator(SplitEnumeratorContext<DistributedSourceSplit> enumContext) {
        return new DistributedSourceEnumerator(enumContext, new ArrayList<>());
    }

    @Override
    public SplitEnumerator<DistributedSourceSplit, Collection<DistributedSourceSplit>>
            restoreEnumerator(
                    SplitEnumeratorContext<DistributedSourceSplit> enumContext,
                    Collection<DistributedSourceSplit> checkpoint) {
        return new DistributedSourceEnumerator(enumContext, new ArrayList<>(checkpoint));
    }

    @Override
    public SimpleVersionedSerializer<DistributedSourceSplit> getSplitSerializer() {
        return new DistributedSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<DistributedSourceSplit>>
            getEnumeratorCheckpointSerializer() {
        return new DistributedSourceEnumeratorSerializer();
    }

    @Override
    public SourceReader<Event, DistributedSourceSplit> createReader(
            SourceReaderContext readerContext) {
        return new DistributedSourceReader(readerContext, numOfTables, distributedTables);
    }

    // ------------------------------ Split ------------------------------

    /** Split for the distributed source, one per subtask. */
    static class DistributedSourceSplit implements SourceSplit {

        final int splitIndex;
        final int parallelism;
        final int eventOffset;

        DistributedSourceSplit(int splitIndex, int parallelism, int eventOffset) {
            this.splitIndex = splitIndex;
            this.parallelism = parallelism;
            this.eventOffset = eventOffset;
        }

        @Override
        public String splitId() {
            return "distributed-split-" + splitIndex;
        }
    }

    // ------------------------------ Enumerator ------------------------------

    /** Enumerator that creates one split per subtask and assigns them by subtask ID. */
    private static class DistributedSourceEnumerator
            implements SplitEnumerator<DistributedSourceSplit, Collection<DistributedSourceSplit>> {

        private final SplitEnumeratorContext<DistributedSourceSplit> context;
        private final List<DistributedSourceSplit> pendingSplits;

        DistributedSourceEnumerator(
                SplitEnumeratorContext<DistributedSourceSplit> context,
                List<DistributedSourceSplit> pendingSplits) {
            this.context = context;
            this.pendingSplits = pendingSplits;
        }

        @Override
        public void start() {
            if (pendingSplits.isEmpty()) {
                int parallelism = context.currentParallelism();
                for (int i = 0; i < parallelism; i++) {
                    pendingSplits.add(new DistributedSourceSplit(i, parallelism, 0));
                }
            }
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            // No-op: splits are assigned in addReader via push model
        }

        @Override
        public void addSplitsBack(List<DistributedSourceSplit> splits, int subtaskId) {
            pendingSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {
            DistributedSourceSplit assigned = null;
            for (int i = 0; i < pendingSplits.size(); i++) {
                if (pendingSplits.get(i).splitIndex == subtaskId) {
                    assigned = pendingSplits.remove(i);
                    break;
                }
            }
            if (assigned != null) {
                context.assignSplit(assigned, subtaskId);
            }
            context.signalNoMoreSplits(subtaskId);
        }

        @Override
        public Collection<DistributedSourceSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(pendingSplits);
        }

        @Override
        public void close() {
            // No-op
        }
    }

    // ------------------------------ Reader ------------------------------

    /** Reader that generates events using the same logic as the old DistributedSourceFunction. */
    private static class DistributedSourceReader
            implements SourceReader<Event, DistributedSourceSplit> {

        private static final Logger LOG = LoggerFactory.getLogger(DistributedSourceReader.class);

        private final SourceReaderContext context;
        private final int numOfTables;
        private final boolean distributedTables;
        private final int subTaskId;

        private int parallelism;
        private List<Event> events;
        private int currentIndex;
        private boolean splitReceived;
        private boolean finished;
        private CompletableFuture<Void> availability;

        DistributedSourceReader(
                SourceReaderContext context, int numOfTables, boolean distributedTables) {
            this.context = context;
            this.numOfTables = numOfTables;
            this.distributedTables = distributedTables;
            this.subTaskId = context.getIndexOfSubtask();
            this.splitReceived = false;
            this.finished = false;
            this.availability = new CompletableFuture<>();
        }

        @Override
        public void start() {
            // Wait for splits to be assigned
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Event> output) throws InterruptedException {
            if (!splitReceived) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            if (currentIndex < events.size()) {
                Event event = events.get(currentIndex++);
                LOG.info("{}> Emitting event {}", subTaskId, event);
                output.collect(event);
                return currentIndex < events.size()
                        ? InputStatus.MORE_AVAILABLE
                        : InputStatus.END_OF_INPUT;
            }
            if (parallelism > 1 && !finished) {
                // To allow test running correctly, we need to wait for downstream schema
                // evolutions to finish before closing any subTask.
                Thread.sleep(10000);
                finished = true;
            }
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public List<DistributedSourceSplit> snapshotState(long checkpointId) {
            if (!splitReceived || finished) {
                return Collections.emptyList();
            }
            return Collections.singletonList(
                    new DistributedSourceSplit(subTaskId, parallelism, currentIndex));
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availability;
        }

        @Override
        public void addSplits(List<DistributedSourceSplit> splits) {
            for (DistributedSourceSplit split : splits) {
                this.parallelism = split.parallelism;
                this.events = generateAllEvents(split);
                this.currentIndex = split.eventOffset;
            }
            this.splitReceived = true;
            this.availability.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            // If no splits were assigned (shouldn't happen), mark as finished
            if (!splitReceived) {
                finished = true;
                availability.complete(null);
            }
        }

        @Override
        public void close() {
            // No-op
        }

        // ----- Event generation logic (migrated from DistributedSourceFunction) -----

        private List<Event> generateAllEvents(DistributedSourceSplit split) {
            int parallelism = split.parallelism;
            int iotaCounter = 0;

            // Initialize dummy data types (same as original open())
            Map<DataType, Object> dummyDataTypes = new LinkedHashMap<>();
            dummyDataTypes.put(DataTypes.BOOLEAN(), true);
            dummyDataTypes.put(DataTypes.TINYINT(), (byte) 17);
            dummyDataTypes.put(DataTypes.SMALLINT(), (short) 34);
            dummyDataTypes.put(DataTypes.INT(), (int) 68);
            dummyDataTypes.put(DataTypes.BIGINT(), (long) 136);
            dummyDataTypes.put(DataTypes.FLOAT(), (float) 272.0);
            dummyDataTypes.put(DataTypes.DOUBLE(), (double) 544.0);
            dummyDataTypes.put(
                    DataTypes.DECIMAL(17, 11),
                    DecimalData.fromBigDecimal(new BigDecimal("1088.000"), 17, 11));
            dummyDataTypes.put(DataTypes.CHAR(17), BinaryStringData.fromString("Alice"));
            dummyDataTypes.put(DataTypes.VARCHAR(17), BinaryStringData.fromString("Bob"));
            dummyDataTypes.put(DataTypes.BINARY(17), "Cicada".getBytes());
            dummyDataTypes.put(DataTypes.VARBINARY(17), "Derrida".getBytes());
            dummyDataTypes.put(DataTypes.TIME(9), TimeData.fromMillisOfDay(64801000));
            dummyDataTypes.put(
                    DataTypes.TIMESTAMP(9),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:00")));
            dummyDataTypes.put(
                    DataTypes.TIMESTAMP_TZ(9),
                    ZonedTimestampData.of(364800000, 123456, "Asia/Shanghai"));
            dummyDataTypes.put(
                    DataTypes.TIMESTAMP_LTZ(9),
                    LocalZonedTimestampData.fromInstant(toInstant("2019-12-31 18:00:00")));

            // Build tables list
            List<TableId> tables;
            if (distributedTables) {
                tables =
                        IntStream.range(0, numOfTables)
                                .mapToObj(
                                        idx ->
                                                TableId.tableId(
                                                        "default_namespace",
                                                        "default_database",
                                                        "table_" + idx))
                                .collect(Collectors.toList());
            } else {
                tables =
                        IntStream.range(0, numOfTables)
                                .mapToObj(
                                        idx ->
                                                TableId.tableId(
                                                        "default_namespace_subtask_" + subTaskId,
                                                        "default_database",
                                                        "table_" + idx))
                                .collect(Collectors.toList());
            }

            // Collect all events
            List<Event> allEvents = new ArrayList<>();
            Map<TableId, Schema> headSchemaMap = new HashMap<>();

            // Use a mutable counter wrapper for iota in lambdas
            int[] iotaRef = {iotaCounter};

            // Phase 1: Create tables and initial data
            Consumer<TableId> createTableConsumer =
                    tableId -> {
                        Schema initialSchema =
                                Schema.newBuilder()
                                        .physicalColumn("id", DataTypes.STRING())
                                        .primaryKey("id")
                                        .partitionKey("id")
                                        .build();
                        CreateTableEvent createTableEvent =
                                new CreateTableEvent(tableId, initialSchema);
                        headSchemaMap.compute(
                                tableId,
                                (tbl, schema) ->
                                        SchemaUtils.applySchemaChangeEvent(
                                                schema, createTableEvent));
                        allEvents.add(createTableEvent);
                        for (int i = 0; i < 10; i++) {
                            allEvents.add(
                                    DataChangeEvent.insertEvent(
                                            tableId,
                                            generateBinRec(
                                                    headSchemaMap.get(tableId),
                                                    dummyDataTypes,
                                                    subTaskId,
                                                    iotaRef)));
                        }
                    };

            if (parallelism > 1) {
                Collections.shuffle(tables);
            }
            tables.forEach(createTableConsumer);

            // Phase 2: Add columns for each data type
            List<DataType> fullTypes = new ArrayList<>(dummyDataTypes.keySet());
            if (parallelism > 1) {
                Collections.shuffle(fullTypes);
            }

            for (DataType colType : fullTypes) {
                // Global column
                if (parallelism > 1) {
                    Collections.shuffle(tables);
                }
                for (TableId tableId : tables) {
                    AddColumnEvent addColumnEvent =
                            new AddColumnEvent(
                                    tableId,
                                    Collections.singletonList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "col_"
                                                                    + colType.getClass()
                                                                            .getSimpleName()
                                                                            .toLowerCase(),
                                                            colType))));
                    headSchemaMap.compute(
                            tableId,
                            (tbl, schema) ->
                                    SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent));
                    allEvents.add(addColumnEvent);
                    allEvents.add(
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    generateBinRec(
                                            headSchemaMap.get(tableId),
                                            dummyDataTypes,
                                            subTaskId,
                                            iotaRef)));
                }

                // Subtask-specific column
                if (parallelism > 1) {
                    Collections.shuffle(tables);
                }
                for (TableId tableId : tables) {
                    AddColumnEvent addColumnEvent =
                            new AddColumnEvent(
                                    tableId,
                                    Collections.singletonList(
                                            new AddColumnEvent.ColumnWithPosition(
                                                    Column.physicalColumn(
                                                            "subtask_"
                                                                    + subTaskId
                                                                    + "_col_"
                                                                    + colType.getClass()
                                                                            .getSimpleName()
                                                                            .toLowerCase(),
                                                            colType))));
                    headSchemaMap.compute(
                            tableId,
                            (tbl, schema) ->
                                    SchemaUtils.applySchemaChangeEvent(schema, addColumnEvent));
                    allEvents.add(addColumnEvent);
                    allEvents.add(
                            DataChangeEvent.insertEvent(
                                    tableId,
                                    generateBinRec(
                                            headSchemaMap.get(tableId),
                                            dummyDataTypes,
                                            subTaskId,
                                            iotaRef)));
                }
            }

            return allEvents;
        }

        private static BinaryRecordData generateBinRec(
                Schema schema, Map<DataType, Object> dummyDataTypes, int subTaskId, int[] iotaRef) {
            BinaryRecordDataGenerator generator =
                    new BinaryRecordDataGenerator(
                            schema.getColumnDataTypes().toArray(new DataType[0]));

            int arity = schema.getColumnDataTypes().size();
            List<DataType> rowTypes = schema.getColumnDataTypes();
            Object[] rowObjects = new Object[arity];

            for (int i = 0; i < arity; i++) {
                DataType type = rowTypes.get(i);
                if (Objects.equals(type, DataTypes.STRING())) {
                    rowObjects[i] =
                            BinaryStringData.fromString(
                                    String.format("__$%d$%d$__", subTaskId, iotaRef[0]++));
                } else {
                    rowObjects[i] = dummyDataTypes.get(type);
                }
            }
            return generator.generate(rowObjects);
        }

        private static Instant toInstant(String ts) {
            return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant();
        }
    }

    // ------------------------------ Serializers ------------------------------

    /** Serializer for {@link DistributedSourceSplit}. */
    private static class DistributedSourceSplitSerializer
            implements SimpleVersionedSerializer<DistributedSourceSplit> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(DistributedSourceSplit split) throws IOException {
            try (ByteArrayOutputStream bao = new ByteArrayOutputStream(12)) {
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(bao);
                view.writeInt(split.splitIndex);
                view.writeInt(split.parallelism);
                view.writeInt(split.eventOffset);
                return bao.toByteArray();
            }
        }

        @Override
        public DistributedSourceSplit deserialize(int version, byte[] serialized)
                throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized)) {
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bis);
                int splitIndex = view.readInt();
                int parallelism = view.readInt();
                int eventOffset = view.readInt();
                return new DistributedSourceSplit(splitIndex, parallelism, eventOffset);
            }
        }
    }

    /** Serializer for the enumerator checkpoint (collection of splits). */
    private static class DistributedSourceEnumeratorSerializer
            implements SimpleVersionedSerializer<Collection<DistributedSourceSplit>> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(Collection<DistributedSourceSplit> splits) throws IOException {
            try (ByteArrayOutputStream bao = new ByteArrayOutputStream(256);
                    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(bao)) {
                view.writeInt(splits.size());
                for (DistributedSourceSplit split : splits) {
                    view.writeInt(split.splitIndex);
                    view.writeInt(split.parallelism);
                    view.writeInt(split.eventOffset);
                }
                return bao.toByteArray();
            }
        }

        @Override
        public Collection<DistributedSourceSplit> deserialize(int version, byte[] serialized)
                throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                    DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bis)) {
                int size = view.readInt();
                List<DistributedSourceSplit> splits = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    int splitIndex = view.readInt();
                    int parallelism = view.readInt();
                    int eventOffset = view.readInt();
                    splits.add(new DistributedSourceSplit(splitIndex, parallelism, eventOffset));
                }
                return splits;
            }
        }
    }
}
