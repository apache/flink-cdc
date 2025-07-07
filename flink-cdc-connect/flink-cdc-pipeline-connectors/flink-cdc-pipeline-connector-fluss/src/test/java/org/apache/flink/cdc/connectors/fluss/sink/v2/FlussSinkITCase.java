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

package org.apache.flink.cdc.connectors.fluss.sink.v2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.fluss.sink.FlussEventSerializationSchema;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static com.alibaba.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for FlussSink. */
public class FlussSinkITCase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    static final String CATALOG_NAME = "test_catalog";
    static final String DEFAULT_DB = "default_db";

    protected TableEnvironment tBatchEnv;

    @BeforeEach
    void before() {
        // open a catalog so that we can get table from the catalog
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();

        // create batch table environment
        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        // create database
        tBatchEnv.executeSql("create database " + DEFAULT_DB);
        tBatchEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tBatchEnv.useDatabase(BUILTIN_DATABASE);
        tBatchEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAllSinkType(boolean primaryKeyTable) throws Exception {
        TableId tableId = TableId.tableId("default_namespace", DEFAULT_DB, "test_all_sink_table");
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    char_type CHAR,\n"
                                        + "    varchar_type VARCHAR,\n"
                                        + "    string_type STRING,\n"
                                        + "    boolean_type BOOLEAN,\n"
                                        + "    decimal_type DECIMAL(6, 2),\n"
                                        + "    tinyint_type TINYINT,\n"
                                        + "    smallint_type SMALLINT,\n"
                                        + "    int_type INT,\n"
                                        + "    bigint_type BIGINT,\n"
                                        + "    float_type FLOAT,\n"
                                        + "    double_type DOUBLE,\n"
                                        + "    date_type DATE,\n"
                                        + "    time_type TIME,\n"
                                        + "    timestamp_type TIMESTAMP,\n"
                                        + "    timestamp_ltz_type TIMESTAMP_LTZ(8)\n"
                                        + (primaryKeyTable
                                                ? " ,PRIMARY KEY (int_type) NOT ENFORCED \n"
                                                : "")
                                        + ");",
                                tableId.getSchemaName(),
                                tableId.getTableName()))
                .await();
        String[] fieldNames =
                new String[] {
                    "char_type",
                    "varchar_type",
                    "string_type",
                    "boolean_type",
                    "decimal_type",
                    "tinyint_type",
                    "smallint_type",
                    "int_type",
                    "bigint_type",
                    "float_type",
                    "double_type",
                    "date_type",
                    "time_type",
                    "timestamp_type",
                    "timestamp_ltz_type"
                };
        String[] pkFieldNames = primaryKeyTable ? new String[] {"int_type"} : new String[0];

        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.CHAR(1),
                    DataTypes.VARCHAR(20),
                    DataTypes.STRING(),
                    DataTypes.BOOLEAN(),
                    DataTypes.DECIMAL(6, 2),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP_LTZ(8)
                };

        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        BinaryStringData.fromString("a"),
                        BinaryStringData.fromString("test character"),
                        BinaryStringData.fromString("test text"),
                        false,
                        DecimalData.fromBigDecimal(new BigDecimal("8119.21"), 6, 2),
                        Byte.valueOf("1"),
                        Short.valueOf("32767"),
                        32768,
                        652482L,
                        20.2007F,
                        8.58965,
                        //  2023-11-12 - 1970-01-01 = 19673 days
                        19673,
                        (8 * 3600 + 30 * 60 + 15) * 1000,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)),
                        LocalZonedTimestampData.fromInstant(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                        .atZone(ZoneId.of("GMT+05:00"))
                                        .toInstant())
                    },
                    new Object[] {
                        null, null, null, null, null, null, null, 0, null, null, null, null, null,
                        null, null
                    }
                };
        // default timezone is asian/shanghai
        List<String> expectedRows =
                Arrays.asList(
                        String.format(
                                "+I[a, test character, test text, false, 8119.21, 1, 32767, 32768, 652482, 20.2007, 8.58965, 2023-11-12, 08:30:15, %s, 2023-11-11T06:11:11.000000011Z]",
                                primaryKeyTable
                                        ? "2023-11-11T11:11:11.000000011"
                                        : "2023-11-11T11:11:11"),
                        "+I[null, null, null, null, null, null, null, 0, null, null, null, null, null, null, null]");

        testInsertSingleTable(
                tableId,
                fieldNames,
                pkFieldNames,
                dataTypes,
                insertedValues,
                expectedRows,
                new String[0]);
    }

    @Test
    void testPartitionTable() throws Exception {
        TableId tableId =
                TableId.tableId("default_namespace", DEFAULT_DB, "test_partition_sink_table");
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    id INT,\n"
                                        + "    ds STRING,\n"
                                        + "    title STRING,\n"
                                        + "    body STRING, \n"
                                        + "     PRIMARY KEY (id, ds) NOT ENFORCED "
                                        + ");",
                                tableId.getSchemaName(), tableId.getTableName()))
                .await();

        String[] fieldNames = new String[] {"id", "ds", "title", "body"};
        String[] pkFieldNames = new String[] {"id", "ds"};
        String[] partitionKeys = {"ds"};
        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
                };
        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        1,
                        BinaryStringData.fromString("20220727"),
                        BinaryStringData.fromString("title_01"),
                        BinaryStringData.fromString("partition_test")
                    }
                };
        String[] expected = new String[] {"+I[1, 20220727, title_01, partition_test]"};
        testInsertSingleTable(
                tableId,
                fieldNames,
                pkFieldNames,
                dataTypes,
                insertedValues,
                Arrays.asList(expected),
                partitionKeys);
    }

    @Test
    void testInsertPkRecordIntoLogTable() throws Exception {
        TableId tableId = TableId.tableId("default_namespace", DEFAULT_DB, "test_pk_to_log_table");
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    id INT,\n"
                                        + "    name STRING \n"
                                        + ");",
                                tableId.getSchemaName(), tableId.getTableName()))
                .await();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(tableId, schema));
        events.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        assertThatThrownBy(() -> submitJob(events))
                .rootCause()
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("UPSERT for log table");
    }

    @Test
    void testInsertNonPkRecordIntoPrimaryTable() throws Exception {
        TableId tableId =
                TableId.tableId("default_namespace", DEFAULT_DB, "test_non_pk_to_primary_table");
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    id INT,\n"
                                        + "    name STRING ,\n"
                                        + "    PRIMARY KEY (id) NOT ENFORCED "
                                        + ");",
                                tableId.getSchemaName(), tableId.getTableName()))
                .await();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(tableId, schema));
        events.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        assertThatThrownBy(() -> submitJob(events))
                .rootCause()
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("APPEND for primary key table");
    }

    @Test
    void testDelete() throws Exception {
        TableId tableId = TableId.tableId("default_namespace", DEFAULT_DB, "test_delete");
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    id INT,\n"
                                        + "    name STRING ,\n"
                                        + "    PRIMARY KEY (id) NOT ENFORCED "
                                        + ");",
                                tableId.getSchemaName(), tableId.getTableName()))
                .await();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(tableId, schema));
        events.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));
        events.add(
                DataChangeEvent.deleteEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));

        submitJob(events);
        checkResult(tableId, Collections.singletonList("+I[2, Bob]"));
    }

    @Test
    void testUpdate() throws Exception {
        TableId tableId = TableId.tableId("default_namespace", DEFAULT_DB, "test_delete");
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s.%s (\n"
                                        + "    id INT,\n"
                                        + "    name STRING ,\n"
                                        + "    PRIMARY KEY (id) NOT ENFORCED "
                                        + ");",
                                tableId.getSchemaName(), tableId.getTableName()))
                .await();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(tableId, schema));
        events.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));

        events.add(
                DataChangeEvent.updateEvent(
                        tableId,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")}),
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Mike")})));

        submitJob(events);
        checkResult(tableId, Arrays.asList("+I[1, Mike]", "+I[2, Bob]"));
    }

    @Test
    void testMultipleTables() throws Exception {
        TableId tableId1 = TableId.tableId("default_namespace", DEFAULT_DB, "test_multiple_1");
        TableId tableId2 = TableId.tableId("default_namespace", DEFAULT_DB, "test_multiple_2");
        String prepareCreateTableSql =
                "CREATE TABLE %s.%s (\n"
                        + "    id INT,\n"
                        + "    ds STRING,\n"
                        + "    title STRING,\n"
                        + "    body STRING, \n"
                        + "    PRIMARY KEY (id, ds) NOT ENFORCED );";
        tBatchEnv
                .executeSql(
                        String.format(
                                prepareCreateTableSql,
                                tableId1.getSchemaName(),
                                tableId1.getTableName()))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                prepareCreateTableSql,
                                tableId2.getSchemaName(),
                                tableId2.getTableName()))
                .await();

        String[] fieldNames = new String[] {"id", "ds", "title", "body"};

        String[] pkFieldNames = new String[] {"id", "ds"};
        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
                };
        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        1,
                        BinaryStringData.fromString("20220727"),
                        BinaryStringData.fromString("title_01"),
                        BinaryStringData.fromString("partition_test")
                    }
                };

        List<Event> events = new ArrayList<>();
        events.addAll(
                constructInsertEvents(
                        tableId1,
                        fieldNames,
                        pkFieldNames,
                        dataTypes,
                        insertedValues,
                        new String[0]));
        events.addAll(
                constructInsertEvents(
                        tableId2,
                        fieldNames,
                        pkFieldNames,
                        dataTypes,
                        insertedValues,
                        new String[0]));

        submitJob(events);
        checkResult(
                tableId1, Collections.singletonList("+I[1, 20220727, title_01, partition_test]"));
        checkResult(
                tableId2, Collections.singletonList("+I[1, 20220727, title_01, partition_test]"));
    }

    private void testInsertSingleTable(
            TableId tableId,
            String[] fieldNames,
            String[] pkFieldNames,
            DataType[] dataTypes,
            Object[][] insertedValues,
            List<String> expectedRows,
            String[] partitionKeys)
            throws Exception {

        List<Event> events =
                constructInsertEvents(
                        tableId,
                        fieldNames,
                        pkFieldNames,
                        dataTypes,
                        insertedValues,
                        partitionKeys);
        submitJob(events);
        checkResult(tableId, expectedRows);
    }

    private List<Event> constructInsertEvents(
            TableId tableId,
            String[] fieldNames,
            String[] pkFieldNames,
            DataType[] dataTypes,
            Object[][] insertedValues,
            String[] partitionKeys) {
        List<Event> events = new ArrayList<>();

        // 1. create table
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 0; i < fieldNames.length; i++) {
            builder.physicalColumn(fieldNames[i], dataTypes[i]);
        }

        org.apache.flink.cdc.common.schema.Schema table1Schema =
                builder.primaryKey(pkFieldNames).partitionKey(partitionKeys).build();
        events.add(new CreateTableEvent(tableId, table1Schema));

        // table event
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));

        for (int i = 0; i < insertedValues.length; i++) {
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, table1dataGenerator.generate(insertedValues[i])));
        }

        return events;
    }

    private void submitJob(List<Event> events) throws Exception {

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Event> source =
                environment.fromData(events, TypeInformation.of(Event.class));

        FlussEventSerializationSchema flussRecordSerializer = new FlussEventSerializationSchema();
        FlussSink<Event> flussSink =
                new FlussSink<>(FLUSS_CLUSTER_EXTENSION.getClientConfig(), flussRecordSerializer);
        source.sinkTo(flussSink);
        environment.execute();
    }

    private void checkResult(TableId tableId, List<String> expectedRows) {
        CloseableIterator<Row> rowIter =
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "select * from %s.%s limit %d",
                                        tableId.getSchemaName(),
                                        tableId.getTableName(),
                                        expectedRows.size()))
                        .collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }
}
