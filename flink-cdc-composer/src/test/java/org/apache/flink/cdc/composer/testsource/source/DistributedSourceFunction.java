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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Distributed source function for testing purposes only. */
public class DistributedSourceFunction extends RichParallelSourceFunction<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedSourceFunction.class);

    private int subTaskId;
    private int parallelism;

    private final int numOfTables;
    private final boolean distributedTables;
    private transient Map<DataType, Object> dummyDataTypes;
    private List<TableId> tables;
    private transient int iotaCounter;

    public DistributedSourceFunction(int numOfTables, boolean distributedTables) {
        this.numOfTables = numOfTables;
        this.distributedTables = distributedTables;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        iotaCounter = 0;
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
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
        dummyDataTypes = new LinkedHashMap<>();
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
    }

    // Generates statically incrementing data, could be used for data integrity verification.
    private BinaryStringData iota() {
        return BinaryStringData.fromString(String.format("__$%d$%d$__", subTaskId, iotaCounter++));
    }

    private void sendFromTables(Consumer<TableId> tableIdConsumer) {
        if (parallelism > 1) {
            // Inject a little randomness in multi-parallelism mode
            Collections.shuffle(tables);
        }
        tables.forEach(tableIdConsumer);
    }

    @Override
    public void run(SourceContext<Event> context) throws InterruptedException {
        Schema initialSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .primaryKey("id")
                        .partitionKey("id")
                        .build();

        Map<TableId, Schema> headSchemaMap = new HashMap<>();

        sendFromTables(
                tableId -> {
                    CreateTableEvent createTableEvent =
                            new CreateTableEvent(tableId, initialSchema);
                    headSchemaMap.compute(
                            tableId,
                            (tbl, schema) ->
                                    SchemaUtils.applySchemaChangeEvent(schema, createTableEvent));
                    collect(context, createTableEvent);
                    for (int i = 0; i < 10; i++) {
                        collect(
                                context,
                                DataChangeEvent.insertEvent(
                                        tableId, generateBinRec(headSchemaMap.get(tableId))));
                    }
                });

        List<DataType> fullTypes = new ArrayList<>(dummyDataTypes.keySet());
        if (parallelism > 1) {
            // Inject randomness in multi-partition mode
            Collections.shuffle(fullTypes);
        }
        fullTypes.forEach(
                colType -> {
                    sendFromTables(
                            tableId -> {
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
                                                SchemaUtils.applySchemaChangeEvent(
                                                        schema, addColumnEvent));
                                collect(context, addColumnEvent);
                                collect(
                                        context,
                                        DataChangeEvent.insertEvent(
                                                tableId,
                                                generateBinRec(headSchemaMap.get(tableId))));
                            });

                    sendFromTables(
                            tableId -> {
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
                                                SchemaUtils.applySchemaChangeEvent(
                                                        schema, addColumnEvent));
                                collect(context, addColumnEvent);
                                collect(
                                        context,
                                        DataChangeEvent.insertEvent(
                                                tableId,
                                                generateBinRec(headSchemaMap.get(tableId))));
                            });
                });

        if (parallelism > 1) {
            // To allow test running correctly, we need to wait for downstream schema evolutions
            // to finish before closing any subTask.
            Thread.sleep(10000);
        }
    }

    @Override
    public void cancel() {
        // Do nothing
    }

    private BinaryRecordData generateBinRec(Schema schema) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        int arity = schema.getColumnDataTypes().size();
        List<DataType> rowTypes = schema.getColumnDataTypes();
        Object[] rowObjects = new Object[arity];

        for (int i = 0; i < arity; i++) {
            DataType type = rowTypes.get(i);
            if (Objects.equals(type, DataTypes.STRING())) {
                rowObjects[i] = iota();
            } else {
                rowObjects[i] = dummyDataTypes.get(type);
            }
        }
        return generator.generate(rowObjects);
    }

    private void collect(SourceContext<Event> sourceContext, Event event) {
        LOG.info("{}> Emitting event {}", subTaskId, event);
        sourceContext.collect(event);
    }

    private Instant toInstant(String ts) {
        return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant();
    }
}
