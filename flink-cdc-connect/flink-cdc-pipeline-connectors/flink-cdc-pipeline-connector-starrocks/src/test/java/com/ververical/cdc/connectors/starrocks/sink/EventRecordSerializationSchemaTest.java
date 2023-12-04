/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververical.cdc.connectors.starrocks.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
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
import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.BooleanType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DateType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.SmallIntType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.utils.SchemaUtils;
import com.ververica.cdc.connectors.starrocks.sink.EventRecordSerializationSchema;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for {@link EventRecordSerializationSchema}. */
public class EventRecordSerializationSchemaTest {

    private EventRecordSerializationSchema serializer;
    private ObjectMapper objectMapper;

    @Before
    public void setup() {
        this.serializer = new EventRecordSerializationSchema(ZoneId.of("+08"));
        this.serializer.open(
                new MockInitializationContext(),
                new DefaultStarRocksSinkContext(
                        new MockInitContext(),
                        new StarRocksSinkOptions(new Configuration(), new HashMap<>())));
        this.objectMapper = new ObjectMapper();
    }

    @After
    public void teardown() {
        this.serializer.close();
    }

    @Test
    public void testMixedSchemaAndDataChanges() throws Exception {
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
        assertNull(serializer.serialize(createTableEvent1));

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
        assertNull(serializer.serialize(createTableEvent2));

        BinaryRecordDataGenerator generator2 =
                new BinaryRecordDataGenerator(
                        schema2.getColumnDataTypes().toArray(new DataType[0]));
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table2,
                        generator2.generate(
                                new Object[] {
                                    (int) LocalDate.of(2023, 11, 27).toEpochDay(),
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
        assertNull(serializer.serialize(addColumnEvent));

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
                serializer.serialize(deleteEvent2));

        // 4. drop columns from table2, and insert data
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(
                        table2,
                        Arrays.asList(
                                Column.physicalColumn("col2", new FloatType()),
                                Column.physicalColumn("col3", new VarCharType(20))));
        Schema newSchema2 = SchemaUtils.applySchemaChangeEvent(schema2, dropColumnEvent);
        BinaryRecordDataGenerator newGenerator2 =
                new BinaryRecordDataGenerator(
                        newSchema2.getColumnDataTypes().toArray(new DataType[0]));
        assertNull(serializer.serialize(dropColumnEvent));

        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table2,
                        newGenerator2.generate(
                                new Object[] {(int) LocalDate.of(2023, 11, 28).toEpochDay()}));
        verifySerializeResult(
                table2, "{\"col1\":\"2023-11-28\",\"__op\":0}", serializer.serialize(insertEvent3));
    }

    private void verifySerializeResult(
            TableId expectTable, String expectRow, StarRocksRowData actualRowData)
            throws Exception {
        assertEquals(expectTable.getSchemaName(), actualRowData.getDatabase());
        assertEquals(expectTable.getTableName(), actualRowData.getTable());
        SortedMap expectMap =
                objectMapper.readValue(expectRow, new TypeReference<TreeMap<String, Object>>() {});
        SortedMap actualMap =
                actualRowData.getRow() == null
                        ? null
                        : objectMapper.readValue(
                                actualRowData.getRow(),
                                new TypeReference<TreeMap<String, Object>>() {});
        assertEquals(expectMap, actualMap);
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
    }
}
