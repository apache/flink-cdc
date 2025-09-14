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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.v2.DefaultStarRocksSinkContext;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Tests for {@link org.apache.flink.cdc.connectors.starrocks.sink.EventRecordSerializationSchema}.
 */
class EventRecordSerializationSchemaTest {

    private EventRecordSerializationSchema serializer;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        this.serializer = new EventRecordSerializationSchema(ZoneId.of("+08"));
        this.serializer.open(
                new MockInitializationContext(),
                new DefaultStarRocksSinkContext(
                        new MockInitContext(),
                        new StarRocksSinkOptions(new Configuration(), new HashMap<>())));
        this.objectMapper = new ObjectMapper();
    }

    @AfterEach
    public void teardown() {
        this.serializer.close();
    }

    @Test
    void testMixedSchemaAndDataChanges() throws Exception {
        // 1. create table1, and insert/delete/update data
        TableId table1 = TableId.parse("test.tbl1");
        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("col1", new IntType())
                        .physicalColumn("col2", new BooleanType())
                        .physicalColumn("col3", new TimestampType())
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent1 = new CreateTableEvent(table1, schema1);
        Assertions.assertThat(serializer.serialize(createTableEvent1)).isNull();

        BinaryRecordDataGenerator generator1 =
                new BinaryRecordDataGenerator(
                        schema1.getColumnDataTypes().toArray(new DataType[0]));
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    1,
                                    true,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 18:00:00"))
                                }));
        verifySerializeResult(
                table1,
                "{\"col1\":1,\"col2\":true,\"col3\":\"2023-11-27 18:00:00\",\"__op\":0}",
                serializer.serialize(insertEvent1));

        DataChangeEvent deleteEvent1 =
                DataChangeEvent.deleteEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    2,
                                    false,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 19:00:00"))
                                }));
        verifySerializeResult(
                table1,
                "{\"col1\":2,\"col2\":false,\"col3\":\"2023-11-27 19:00:00\",\"__op\":1}",
                serializer.serialize(deleteEvent1));

        DataChangeEvent updateEvent1 =
                DataChangeEvent.updateEvent(
                        table1,
                        generator1.generate(
                                new Object[] {
                                    3,
                                    false,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 20:00:00"))
                                }),
                        generator1.generate(
                                new Object[] {
                                    3,
                                    true,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 21:00:00"))
                                }));
        verifySerializeResult(
                table1,
                "{\"col1\":3,\"col2\":true,\"col3\":\"2023-11-27 21:00:00\",\"__op\":0}",
                serializer.serialize(updateEvent1));

        // 2. create table2, and insert data
        TableId table2 = TableId.parse("test.tbl2");
        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("col1", new DateType())
                        .physicalColumn("col2", new FloatType())
                        .physicalColumn("col3", new VarCharType(20))
                        .primaryKey("col1")
                        .build();
        CreateTableEvent createTableEvent2 = new CreateTableEvent(table2, schema2);
        Assertions.assertThat(serializer.serialize(createTableEvent2)).isNull();

        BinaryRecordDataGenerator generator2 =
                new BinaryRecordDataGenerator(
                        schema2.getColumnDataTypes().toArray(new DataType[0]));
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table2,
                        generator2.generate(
                                new Object[] {
                                    DateData.fromLocalDate(LocalDate.of(2023, 11, 27)),
                                    3.4f,
                                    BinaryStringData.fromString("insert table2")
                                }));
        verifySerializeResult(
                table2,
                "{\"col1\":\"2023-11-27\",\"col2\":3.4,\"col3\":\"insert table2\",\"__op\":0}",
                serializer.serialize(insertEvent2));

        // 3. add columns to table1, and delete data
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        table1,
                        Arrays.asList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col4", new DecimalType(20, 5))),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("col5", new SmallIntType())),
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn(
                                                "col6", new LocalZonedTimestampType()))));
        Schema newSchema1 = SchemaUtils.applySchemaChangeEvent(schema1, addColumnEvent);
        BinaryRecordDataGenerator newGenerator1 =
                new BinaryRecordDataGenerator(
                        newSchema1.getColumnDataTypes().toArray(new DataType[0]));
        Assertions.assertThat(serializer.serialize(addColumnEvent)).isNull();

        DataChangeEvent deleteEvent2 =
                DataChangeEvent.deleteEvent(
                        table1,
                        newGenerator1.generate(
                                new Object[] {
                                    4,
                                    true,
                                    TimestampData.fromTimestamp(
                                            Timestamp.valueOf("2023-11-27 21:00:00")),
                                    DecimalData.fromBigDecimal(new BigDecimal("83.23"), 20, 5),
                                    (short) 9,
                                    LocalZonedTimestampData.fromInstant(
                                            LocalDateTime.of(2023, 11, 27, 21, 0, 0)
                                                    .toInstant(ZoneOffset.of("+10")))
                                }));
        verifySerializeResult(
                table1,
                "{\"col1\":4,\"col2\":true,\"col3\":\"2023-11-27 21:00:00\",\"col4\":83.23,\"col5\":9,\"col6\":\"2023-11-27 19:00:00\",\"__op\":1}",
                Objects.requireNonNull(serializer.serialize(deleteEvent2)));

        // 4. drop columns from table2, and insert data
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table2, Arrays.asList("col2", "col3"));
        Schema newSchema2 = SchemaUtils.applySchemaChangeEvent(schema2, dropColumnEvent);
        BinaryRecordDataGenerator newGenerator2 =
                new BinaryRecordDataGenerator(
                        newSchema2.getColumnDataTypes().toArray(new DataType[0]));
        Assertions.assertThat(serializer.serialize(dropColumnEvent)).isNull();

        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table2,
                        newGenerator2.generate(
                                new Object[] {DateData.fromLocalDate(LocalDate.of(2023, 11, 28))}));
        verifySerializeResult(
                table2,
                "{\"col1\":\"2023-11-28\",\"__op\":0}",
                Objects.requireNonNull(serializer.serialize(insertEvent3)));
    }

    private void verifySerializeResult(
            TableId expectTable, String expectRow, StarRocksRowData actualRowData)
            throws Exception {
        Assertions.assertThat(actualRowData.getDatabase()).isEqualTo(expectTable.getSchemaName());
        Assertions.assertThat(actualRowData.getTable()).isEqualTo(expectTable.getTableName());
        SortedMap<String, Object> expectMap =
                objectMapper.readValue(expectRow, new TypeReference<TreeMap<String, Object>>() {});
        SortedMap<String, Object> actualMap =
                actualRowData.getRow() == null
                        ? null
                        : objectMapper.readValue(
                                actualRowData.getRow(),
                                new TypeReference<TreeMap<String, Object>>() {});
        Assertions.assertThat(actualMap).isEqualTo(expectMap);
    }

    /** A mock context for serialization schema testing. */
    private static class MockInitializationContext
            implements SerializationSchema.InitializationContext {

        @Override
        public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(
                    MockInitializationContext.class.getClassLoader());
        }
    }

    private static class MockInitContext implements Sink.InitContext {

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(MockInitContext.class.getClassLoader());
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSubtaskId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getAttemptNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isObjectReuseEnabled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobID getJobId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobInfo getJobInfo() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TaskInfo getTaskInfo() {
            throw new UnsupportedOperationException();
        }
    }
}
