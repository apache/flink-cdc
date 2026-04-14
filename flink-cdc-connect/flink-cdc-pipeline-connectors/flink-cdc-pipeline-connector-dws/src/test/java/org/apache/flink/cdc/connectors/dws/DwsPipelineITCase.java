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

package org.apache.flink.cdc.connectors.dws;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.dws.utils.DwsContainer;
import org.apache.flink.cdc.connectors.dws.utils.DwsSinkTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.LOG_SWITCH;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.URL;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.WRITE_MODE;

/** Integration tests for DWS pipeline sink. */
class DwsPipelineITCase extends DwsSinkTestBase {

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private static final int DATA_FETCHING_TIMEOUT = 10;

    @BeforeAll
    public static void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @BeforeEach
    public void initializeDatabaseAndTable() {

        createTable(
                DwsContainer.DWS_DATABASE,
                DwsContainer.DWS_TABLE_NAME,
                "id",
                new ArrayList<>(
                        Arrays.asList(
                                "id INT NOT NULL",
                                "id1 TINYINT NOT NULL",
                                "id2 SMALLINT NOT NULL",
                                "id3 SMALLSERIAL NOT NULL",
                                "id4 INTEGER NOT NULL",
                                "id5 SERIAL NOT NULL",
                                "id6 BIGINT NOT NULL",
                                "id7 BIGSERIAL NOT NULL",
                                "id8 REAL NOT NULL",
                                "number1 DECIMAL(10,4)",
                                "number2 NUMBER(10,4)",
                                "number3 NUMERIC(10,4)",
                                "bool1 BOOLEAN",
                                "name VARCHAR(51)",
                                "name1 CHAR(11)",
                                "name2 CHARACTER(11)",
                                //                        "name3 NCHAR(11)",
                                //                        "name3 VARCHAR2(11)",
                                //                        "name3 NVARCHAR(11)",
                                //                        "name3 NVARCHAR2(11)",
                                "name3 TEXT",
                                //                        "name3 CLOB",
                                "birthday TIMESTAMPTZ",
                                //                        "birthday1 DATE"
                                "birthday1 TIME",
                                "birthday2 TIMESTAMP")));
        LOG.info("Table {} created.", DwsContainer.DWS_TABLE_NAME);
    }

    @AfterEach
    public void destroyDatabaseAndTable() {
        dropTable(DwsContainer.DWS_DATABASE, DwsContainer.DWS_TABLE_NAME);
        LOG.info("Table {} destroyed.", DwsContainer.DWS_TABLE_NAME);
    }

    private List<Event> generateEvents(TableId tableId) {
        LocalDateTime time1 =
                LocalDateTime.ofInstant(Instant.parse("2021-01-01T08:00:00Z"), ZoneId.of("Z"));
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("id1", DataTypes.TINYINT().notNull(), null))
                        .column(new PhysicalColumn("id2", DataTypes.SMALLINT().notNull(), null))
                        .column(new PhysicalColumn("id3", DataTypes.SMALLINT().notNull(), null))
                        .column(new PhysicalColumn("id4", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("id5", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("id6", DataTypes.BIGINT().notNull(), null))
                        .column(new PhysicalColumn("id7", DataTypes.BIGINT().notNull(), null))
                        .column(new PhysicalColumn("id8", DataTypes.FLOAT().notNull(), null))
                        .column(new PhysicalColumn("number1", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("number2", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("number3", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("bool1", DataTypes.BOOLEAN(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .column(new PhysicalColumn("name1", DataTypes.CHAR(17), null))
                        .column(new PhysicalColumn("name2", DataTypes.CHAR(17), null))
                        //                        .column(new PhysicalColumn("name3",
                        // DataTypes.CHAR(17), null))
                        .column(new PhysicalColumn("name3", DataTypes.STRING(), null))
                        .column(new PhysicalColumn("birthday", DataTypes.TIMESTAMP_LTZ(6), null))
                        //                        .column(new PhysicalColumn("birthday1",
                        // DataTypes.DATE(), null))
                        .column(new PhysicalColumn("birthday1", DataTypes.TIME(6), null))
                        .column(new PhysicalColumn("birthday2", DataTypes.TIMESTAMP(6), null))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.TINYINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                DataTypes.FLOAT(),
                                DataTypes.DOUBLE(),
                                DataTypes.DOUBLE(),
                                DataTypes.DOUBLE(),
                                DataTypes.BOOLEAN(),
                                DataTypes.VARCHAR(17),
                                DataTypes.CHAR(17),
                                DataTypes.CHAR(17),
                                DataTypes.CHAR(17),
                                DataTypes.TIMESTAMP_LTZ(6),
                                //                                DataTypes.DATE()
                                DataTypes.TIME(6),
                                DataTypes.TIMESTAMP(6)));

        return new ArrayList<>(
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            17, // id - INT
                                            (byte) 100, // id1 - TINYINT
                                            (short) 1, // id2 - SMALLINT
                                            (short) 2, // id3 - SMALLINT
                                            1000, // id4 - INT
                                            10000, // id5 - INT
                                            10000L, // id6 - BIGINT
                                            10001L, // id7 - BIGINT
                                            3.15f, // id8 - FLOAT
                                            3.14, // number1 - DOUBLE
                                            3.14, // number2 - DOUBLE
                                            3.15, // number3 - DOUBLE
                                            true, // bool1 - BOOLEAN
                                            BinaryStringData.fromString("GAUSS Day"),
                                            BinaryStringData.fromString("GAUSS Day 1"),
                                            BinaryStringData.fromString("GAUSS Day 2"),
                                            BinaryStringData.fromString("GAUSS Day 3"),
                                            LocalZonedTimestampData.fromInstant(
                                                    Instant.parse("2023-01-01T00:00:00.000Z")),
                                            3661123,
                                            TimestampData.fromLocalDateTime(time1)
                                        })),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            19, // id - INT
                                            (byte) 109, // id1 - TINYINT
                                            (short) 19, // id2 - SMALLINT
                                            (short) 29, // id3 - SMALLINT
                                            1002, // id4 - INT
                                            10002, // id5 - INT
                                            10002L, // id6 - BIGINT
                                            10002L, // id7 - BIGINT
                                            3.16f, // id8 - FLOAT
                                            3.16, // number2 - DOUBLE
                                            3.16, // number2 - DOUBLE
                                            3.17, // number3 - DOUBLE
                                            true, // bool1 - BOOLEAN
                                            BinaryStringData.fromString("Que Sera Sera"),
                                            BinaryStringData.fromString("Que Sera S1"),
                                            BinaryStringData.fromString("Que Sera S2"),
                                            BinaryStringData.fromString("Que Sera S3"),
                                            LocalZonedTimestampData.fromInstant(
                                                    Instant.parse("2023-01-02T00:00:00.000Z")),
                                            3661123,
                                            TimestampData.fromLocalDateTime(time1)
                                        })),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            21, // id - INT
                                            (byte) 102, // id1 - TINYINT
                                            (short) 12, // id2 - SMALLINT
                                            (short) 22, // id3 - SMALLINT
                                            1003, // id4 - INT
                                            10003, // id5 - INT
                                            10003L, // id6 - BIGINT
                                            10003L, // id7 - BIGINT
                                            3.17f, // id8 - FLOAT
                                            3.17, // number1 - DOUBLE
                                            3.17, // number2 - DOUBLE
                                            3.18, // number3 - DOUBLE
                                            true, // bool1 - BOOLEAN
                                            BinaryStringData.fromString("Disenchanted"),
                                            BinaryStringData.fromString("Disenchant1"),
                                            BinaryStringData.fromString("Disenchant2"),
                                            BinaryStringData.fromString("Disenchant3"),
                                            LocalZonedTimestampData.fromInstant(
                                                    Instant.parse("2023-01-03T00:00:00.000Z")),
                                            3661123,
                                            TimestampData.fromLocalDateTime(time1)
                                        })),
                        DataChangeEvent.updateEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            17, // id - INT
                                            (byte) 103, // id1 - TINYINT
                                            (short) 13, // id2 - SMALLINT
                                            (short) 23, // id3 - SMALLINT
                                            1004, // id4 - INT
                                            10004, // id5 - INT
                                            10004L, // id6 - BIGINT
                                            10004L, // id7 - BIGINT
                                            3.18f, // id8 - FLOAT
                                            3.18, // number1 - DOUBLE
                                            3.18, // number2 - DOUBLE
                                            3.19, // number3 - DOUBLE
                                            true, // bool1 - BOOLEAN
                                            BinaryStringData.fromString("GAUSS Day"),
                                            BinaryStringData.fromString("GAUSS Day 1"),
                                            BinaryStringData.fromString("GAUSS Day 2"),
                                            BinaryStringData.fromString("GAUSS Day 3"),
                                            LocalZonedTimestampData.fromInstant(
                                                    Instant.parse("2023-01-04T00:00:00.000Z")),
                                            3661123,
                                            TimestampData.fromLocalDateTime(time1)
                                        }),
                                generator.generate(
                                        new Object[] {
                                            17, // id - INT
                                            (byte) 104, // id1 - TINYINT
                                            (short) 14, // id2 - SMALLINT
                                            (short) 24, // id3 - SMALLINT
                                            1005, // id4 - INT
                                            10005, // id5 - INT
                                            10005L, // id6 - BIGINT
                                            10005L, // id7 - BIGINT
                                            3.18f, // id8 - FLOAT
                                            3.18, // number1 - DOUBLE
                                            3.18, // number2 - DOUBLE
                                            3.19, // number3 - DOUBLE
                                            false, // bool1 - BOOLEAN
                                            BinaryStringData.fromString("GAUSS Day,\t\n"),
                                            BinaryStringData.fromString("GAUSS Day 1"),
                                            BinaryStringData.fromString("GAUSS Day 2"),
                                            BinaryStringData.fromString("GAUSS Day 3"),
                                            LocalZonedTimestampData.fromInstant(
                                                    Instant.parse("2023-01-05T10:00:00.000Z")),
                                            3661123,
                                            TimestampData.fromLocalDateTime(time1)
                                        })),
                        DataChangeEvent.deleteEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            19, // id - INT
                                            (byte) 112, // id1 - TINYINT
                                            (short) 22, // id2 - SMALLINT
                                            (short) 32, // id3 - SMALLINT
                                            1013, // id4 - INT
                                            10013, // id5 - INT
                                            10013L, // id6 - BIGINT
                                            10013L, // id7 - BIGINT
                                            3.27f, // id8 - FLOAT
                                            3.27, // number1 - DOUBLE
                                            3.27, // number2 - DOUBLE
                                            3.28, // number3 - DOUBLE
                                            true, // bool1 - BOOLEAN
                                            BinaryStringData.fromString("Que Sera Sera"),
                                            BinaryStringData.fromString("Que Sera S1"),
                                            BinaryStringData.fromString("Que Sera S2"),
                                            BinaryStringData.fromString("Que Sera S3"),
                                            LocalZonedTimestampData.fromInstant(
                                                    Instant.parse("2023-01-06T00:00:00.000Z")),
                                            3661123,
                                            TimestampData.fromLocalDateTime(time1)
                                        }))));
    }

    private void runValuesToDwsJob() throws Exception {

        TableId tableId =
                TableId.tableId(
                        DwsContainer.DWS_DATABASE,
                        DwsContainer.DWS_SCHEMA,
                        DwsContainer.DWS_TABLE_NAME);

        DataStream<Event> stream =
                env.fromData(generateEvents(tableId), TypeInformation.of(Event.class));

        Configuration config =
                new Configuration()
                        .set(URL, DWS_CONTAINER.getJdbcUrl())
                        .set(USERNAME, DwsContainer.DWS_USERNAME)
                        .set(PASSWORD, DwsContainer.DWS_PASSWORD)
                        .set(LOG_SWITCH, false)
                        .set(WRITE_MODE, "copy_merge")
                        .set(SINK_ENABLE_DELETE, true);

        LOG.info(
                "Test configuration: URL={}, Username={}",
                DWS_CONTAINER.getJdbcUrl(),
                DwsContainer.DWS_USERNAME);

        //        config.addAll(
        //                Configuration.fromMap(
        //
        // Collections.singletonMap("table.create.properties.replication_num", "1")));

        EventSinkProvider eventSinkProvider = createDwsDataSink(config).getEventSinkProvider();

        if (eventSinkProvider instanceof FlinkSinkProvider) {
            Sink<Event> dwsSink = ((FlinkSinkProvider) eventSinkProvider).getSink();
            stream.sinkTo(dwsSink);
        } else if (eventSinkProvider instanceof FlinkSinkFunctionProvider) {
            FlinkSinkFunctionProvider sinkFunctionProvider =
                    (FlinkSinkFunctionProvider) eventSinkProvider;
            SinkFunction<Event> dwsSinkFunction = sinkFunctionProvider.getSinkFunction();
            stream.addSink(dwsSinkFunction);
        } else {
            throw new IllegalStateException(
                    "Unknown EventSinkProvider type: " + eventSinkProvider.getClass());
        }

        LOG.info("Starting Flink job.");
        env.execute("Values to DWS Sink");
        LOG.info("Flink job finished.");

        LOG.info("Inspecting table schema and data.");
        try {
            List<String> tableSchema = inspectTableSchema(tableId);
            LOG.info("Table schema: {}", tableSchema);
        } catch (Exception e) {
            LOG.error("Failed to inspect table schema: {}", e.getMessage(), e);
        }

        List<String> expected =
                new ArrayList<>(
                        Arrays.asList(
                                // JDBC returns REAL columns with IEEE-754 precision expansion.
                                "17 | 104 | 14 | 24 | 1005 | 10005 | 10005 | 10005 | 3.18000007 | 3.1800 | 3.1800 | 3.1900 | f | GAUSS Day,\t\n | GAUSS Day 1 | GAUSS Day 2 | GAUSS Day 3 | 2023-01-05 10:00:00+08 | 01:01:01.123 | 2021-01-01 08:00:00",
                                "21 | 102 | 12 | 22 | 1003 | 10003 | 10003 | 10003 | 3.17000008 | 3.1700 | 3.1700 | 3.1800 | t | Disenchanted | Disenchant1 | Disenchant2 | Disenchant3 | 2023-01-03 00:00:00+08 | 01:01:01.123 | 2021-01-01 08:00:00"));

        LOG.info("Expected records: {}", expected);

        long timeout = System.currentTimeMillis() + DATA_FETCHING_TIMEOUT * 1000;
        int attemptCount = 0;

        while (System.currentTimeMillis() < timeout) {
            attemptCount++;
            List<String> actual = fetchTableContent(tableId, 20);
            LOG.info("Attempt {} fetched {} records: {}", attemptCount, actual.size(), actual);

            if (actual.size() >= expected.size()) {
                LOG.info("Fetched enough records. Start validation.");
                assertEqualsInAnyOrder(expected, actual);
                return;
            }

            LOG.warn(
                    "Attempt {} expected {} records but found {}. Retrying in 1 second.",
                    attemptCount,
                    expected.size(),
                    actual.size());
            Thread.sleep(1000L);
        }

        List<String> finalActual = fetchTableContent(tableId, 15);
        LOG.error(
                "Timed out waiting for records. Expected {}, actual {}.",
                expected.size(),
                finalActual.size());
        LOG.error("Expected records: {}", expected);
        LOG.error("Actual records: {}", finalActual);

        throw new TimeoutException(
                "Failed to fetch enough records in time. Expected: "
                        + expected.size()
                        + ", Actual: "
                        + finalActual.size());
    }
}
