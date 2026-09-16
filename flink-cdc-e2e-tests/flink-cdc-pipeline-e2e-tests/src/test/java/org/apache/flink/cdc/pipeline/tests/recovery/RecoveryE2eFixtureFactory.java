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

package org.apache.flink.cdc.pipeline.tests.recovery;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.source.RuntimeContextAdapter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Test-only source and sink fixture packaged into the pipeline job JAR. */
public class RecoveryE2eFixtureFactory implements DataSourceFactory, DataSinkFactory {

    public static final String IDENTIFIER = "recovery-e2e";

    public static final ConfigOption<String> EVIDENCE_PATH =
            ConfigOptions.key("evidence-path").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> PARALLEL_METADATA_SOURCE =
            ConfigOptions.key("parallel-metadata-source").booleanType().defaultValue(false);

    @Override
    public DataSource createDataSource(DataSourceFactory.Context context) {
        return new RecoveryE2eDataSource(
                context.getFactoryConfiguration().get(EVIDENCE_PATH),
                context.getFactoryConfiguration().get(PARALLEL_METADATA_SOURCE));
    }

    @Override
    public DataSink createDataSink(DataSinkFactory.Context context) {
        return new RecoveryE2eDataSink(context.getFactoryConfiguration().get(EVIDENCE_PATH));
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(EVIDENCE_PATH);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(PARALLEL_METADATA_SOURCE);
    }

    public static Class<?>[] fixtureClasses() {
        return new Class<?>[] {
            RecoveryE2eFixtureFactory.class,
            RecoveryE2eDataSource.class,
            RecoveryE2eDataSink.class,
            RecoveryE2eSourceFunction.class,
            RecoveryE2eSinkFunction.class,
            RecoveryE2eMetadataApplier.class,
            RecoveryE2eEvents.class,
            RecoveryE2eEvidence.class
        };
    }
}

class RecoveryE2eDataSource implements DataSource {

    private final String evidencePath;
    private final boolean parallelMetadataSource;

    RecoveryE2eDataSource(String evidencePath, boolean parallelMetadataSource) {
        this.evidencePath = evidencePath;
        this.parallelMetadataSource = parallelMetadataSource;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        return FlinkSourceFunctionProvider.of(new RecoveryE2eSourceFunction(evidencePath));
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        throw new UnsupportedOperationException("Recovery E2E source has no metadata accessor.");
    }

    @Override
    public boolean isParallelMetadataSource() {
        return parallelMetadataSource;
    }
}

class RecoveryE2eDataSink implements DataSink {

    private final String evidencePath;

    RecoveryE2eDataSink(String evidencePath) {
        this.evidencePath = evidencePath;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkFunctionProvider.of(new RecoveryE2eSinkFunction(evidencePath));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new RecoveryE2eMetadataApplier(evidencePath);
    }
}

class RecoveryE2eSourceFunction extends RichParallelSourceFunction<Event>
        implements CheckpointedFunction, CheckpointListener {

    private static final int ADD_FIRST_COLUMN_POSITION = 2;

    private final String evidencePath;
    private transient ListState<Integer> cursorState;
    private transient ListState<Long> checkpointIdState;
    private transient Map<Long, Integer> checkpointCursors;
    private volatile boolean running = true;
    private volatile boolean checkpointCompleted;
    private int cursor;
    private long restoredCheckpointId = -1L;
    private boolean restored;

    RecoveryE2eSourceFunction(String evidencePath) {
        this.evidencePath = evidencePath;
    }

    @Override
    public void run(SourceFunction.SourceContext<Event> context) throws Exception {
        if (RuntimeContextAdapter.getIndexOfThisSubtask(
                        (StreamingRuntimeContext) getRuntimeContext())
                != 0) {
            return;
        }

        RecoveryE2eEvidence.append(evidencePath, "SOURCE\tattempt");
        List<Event> events = RecoveryE2eEvents.events();
        if (restored) {
            synchronized (context.getCheckpointLock()) {
                context.collect(events.get(0));
            }
            RecoveryE2eEvidence.append(evidencePath, "SOURCE\tcreate-replayed");
        }

        while (running && cursor < events.size()) {
            if (cursor == ADD_FIRST_COLUMN_POSITION && !checkpointCompleted) {
                Thread.sleep(10L);
                continue;
            }
            if (cursor == ADD_FIRST_COLUMN_POSITION) {
                synchronized (context.getCheckpointLock()) {
                    RecoveryE2eEvidence.append(evidencePath, "SOURCE\textra_v1-emitted");
                    context.collect(events.get(cursor));
                    if (restored) {
                        cursor++;
                    }
                }
                while (running && !restored) {
                    Thread.sleep(10L);
                }
                continue;
            }
            synchronized (context.getCheckpointLock()) {
                context.collect(events.get(cursor));
                cursor++;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        cursorState.update(Collections.singletonList(cursor));
        checkpointIdState.update(Collections.singletonList(context.getCheckpointId()));
        checkpointCursors.put(context.getCheckpointId(), cursor);
        if (isEmitterSubtask()) {
            RecoveryE2eEvidence.append(
                    evidencePath,
                    "SOURCE\tcheckpoint-snapshot\t"
                            + context.getCheckpointId()
                            + "\tcursor="
                            + cursor);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        cursorState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("event-cursor", Types.INT));
        checkpointIdState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("checkpoint-id", Types.LONG));
        checkpointCursors = new HashMap<>();
        for (Integer restoredCursor : cursorState.get()) {
            cursor = restoredCursor;
        }
        for (Long checkpointId : checkpointIdState.get()) {
            restoredCheckpointId = checkpointId;
        }
        restored = context.isRestored();
        if (restored && isEmitterSubtask()) {
            RecoveryE2eEvidence.append(
                    evidencePath,
                    "SOURCE\tcheckpoint-restored\t" + restoredCheckpointId + "\tcursor=" + cursor);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        Integer checkpointCursor = checkpointCursors.remove(checkpointId);
        if (checkpointCursor == null || checkpointCursor != ADD_FIRST_COLUMN_POSITION) {
            return;
        }
        if (isEmitterSubtask()) {
            RecoveryE2eEvidence.append(
                    evidencePath,
                    "SOURCE\tcheckpoint-completed\t"
                            + checkpointId
                            + "\tcursor="
                            + checkpointCursor);
        }
        checkpointCompleted = true;
    }

    private boolean isEmitterSubtask() {
        return RuntimeContextAdapter.getIndexOfThisSubtask(
                        (StreamingRuntimeContext) getRuntimeContext())
                == 0;
    }
}

class RecoveryE2eSinkFunction implements SinkFunction<Event> {

    private final String evidencePath;

    RecoveryE2eSinkFunction(String evidencePath) {
        this.evidencePath = evidencePath;
    }

    @Override
    public void invoke(Event event, Context context) {
        if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            RecoveryE2eEvidence.append(
                    evidencePath,
                    "DATA\t"
                            + dataChangeEvent.op()
                            + "\t"
                            + format(dataChangeEvent.before())
                            + "\t=>\t"
                            + format(dataChangeEvent.after()));
        }
    }

    private static String format(RecordData record) {
        if (record == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < record.getArity(); index++) {
            if (index > 0) {
                builder.append(',');
            }
            if (!record.isNullAt(index)) {
                builder.append(record.getString(index));
            }
        }
        return builder.toString();
    }
}

class RecoveryE2eMetadataApplier implements MetadataApplier {

    private final String evidencePath;

    RecoveryE2eMetadataApplier(String evidencePath) {
        this.evidencePath = evidencePath;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (schemaChangeEvent instanceof CreateTableEvent) {
            RecoveryE2eEvidence.initializeSchema(evidencePath, "id,value");
            RecoveryE2eEvidence.append(evidencePath, "METADATA\tcreate");
            return;
        }
        if (!(schemaChangeEvent instanceof AddColumnEvent)) {
            return;
        }

        String columnName =
                ((AddColumnEvent) schemaChangeEvent)
                        .getAddedColumns()
                        .get(0)
                        .getAddColumn()
                        .getName();
        if ("extra_v1".equals(columnName)) {
            if (RecoveryE2eEvidence.createMarker(evidencePath + ".extra-v1-applied")) {
                RecoveryE2eEvidence.writeSchema(evidencePath, "id,value,extra_v1");
                RecoveryE2eEvidence.append(evidencePath, "METADATA\textra_v1-applied");
                throw new RuntimeException("Fail after applying extra_v1 without rollback.");
            }
            RecoveryE2eEvidence.append(evidencePath, "METADATA\textra_v1-replayed");
            return;
        }
        if ("extra_v2".equals(columnName)) {
            RecoveryE2eEvidence.writeSchema(evidencePath, "id,value,extra_v1,extra_v2");
            RecoveryE2eEvidence.append(evidencePath, "METADATA\textra_v2-applied");
        }
    }
}

class RecoveryE2eEvents {

    private RecoveryE2eEvents() {}

    static List<Event> events() {
        TableId tableId = TableId.tableId("recovery", "schema", "orders");
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator twoColumns =
                new BinaryRecordDataGenerator(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()});
        BinaryRecordDataGenerator threeColumns =
                new BinaryRecordDataGenerator(
                        new DataType[] {
                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
                        });
        BinaryRecordDataGenerator fourColumns =
                new BinaryRecordDataGenerator(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()
                        });
        return Arrays.asList(
                new CreateTableEvent(tableId, initialSchema),
                DataChangeEvent.insertEvent(
                        tableId,
                        twoColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("before")
                                })),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("extra_v1", DataTypes.STRING())))),
                DataChangeEvent.updateEvent(
                        tableId,
                        threeColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("before"),
                                    null
                                }),
                        threeColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("after"),
                                    BinaryStringData.fromString("v1")
                                })),
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("extra_v2", DataTypes.STRING())))),
                DataChangeEvent.updateEvent(
                        tableId,
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("after"),
                                    BinaryStringData.fromString("v1"),
                                    null
                                }),
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("after"),
                                    BinaryStringData.fromString("v1"),
                                    BinaryStringData.fromString("v2")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("deleted"),
                                    BinaryStringData.fromString("v1"),
                                    BinaryStringData.fromString("v2")
                                })),
                DataChangeEvent.deleteEvent(
                        tableId,
                        fourColumns.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("deleted"),
                                    BinaryStringData.fromString("v1"),
                                    BinaryStringData.fromString("v2")
                                })));
    }
}

class RecoveryE2eEvidence {

    private RecoveryE2eEvidence() {}

    static void append(String evidencePath, String line) {
        try {
            Files.write(
                    Paths.get(evidencePath + ".events"),
                    (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static boolean createMarker(String markerPath) {
        try {
            Files.createFile(Paths.get(markerPath));
            return true;
        } catch (FileAlreadyExistsException ignored) {
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void initializeSchema(String evidencePath, String schema) {
        Path schemaPath = Paths.get(evidencePath + ".schema");
        if (!Files.exists(schemaPath)) {
            writeSchema(evidencePath, schema);
        }
    }

    static void writeSchema(String evidencePath, String schema) {
        try {
            Files.write(
                    Paths.get(evidencePath + ".schema"),
                    schema.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
