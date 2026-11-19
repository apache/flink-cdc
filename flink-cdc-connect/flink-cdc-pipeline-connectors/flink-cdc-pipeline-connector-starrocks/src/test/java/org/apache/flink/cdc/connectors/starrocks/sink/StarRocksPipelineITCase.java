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
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksContainer;
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksSinkTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.JDBC_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.LOAD_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.USERNAME;

/** IT tests for {@link StarRocksDataSink}. */
class StarRocksPipelineITCase extends StarRocksSinkTestBase {
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        RestartStrategyUtils.configureNoRestartStrategy(env);
    }

    @BeforeEach
    public void initializeDatabaseAndTable() {
        executeSql(
                String.format(
                        "CREATE DATABASE IF NOT EXISTS `%s`;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME));

        LOG.info("Database {} created.", StarRocksContainer.STARROCKS_DATABASE_NAME);

        List<String> schema =
                Arrays.asList(
                        "id INT NOT NULL",
                        "number DOUBLE",
                        "name VARCHAR(51)",
                        "birthday DATETIME");

        executeSql(
                String.format(
                        "CREATE TABLE `%s`.`%s` (%s) PRIMARY KEY (`%s`) DISTRIBUTED BY HASH(`%s`) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\");",
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME,
                        String.join(", ", schema),
                        "id",
                        "id"));

        LOG.info("Table {} created.", StarRocksContainer.STARROCKS_TABLE_NAME);
    }

    @AfterEach
    public void destroyDatabaseAndTable() {

        executeSql(
                String.format(
                        "DROP TABLE %s.%s;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME));

        LOG.info("Table {} destroyed.", StarRocksContainer.STARROCKS_TABLE_NAME);

        executeSql(String.format("DROP DATABASE %s;", StarRocksContainer.STARROCKS_DATABASE_NAME));

        LOG.info("Database {} destroyed.", StarRocksContainer.STARROCKS_DATABASE_NAME);
    }

    private List<Event> generateEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .column(new PhysicalColumn("birthday", DataTypes.TIMESTAMP_LTZ(6), null))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.DOUBLE(),
                                DataTypes.VARCHAR(17),
                                DataTypes.TIMESTAMP_LTZ(6)));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17,
                                    3.14,
                                    BinaryStringData.fromString("StarRocks"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19,
                                    2.718,
                                    BinaryStringData.fromString("Que Sera Sera"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    21,
                                    1.732,
                                    BinaryStringData.fromString("Disenchanted"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19,
                                    2.718,
                                    BinaryStringData.fromString("Que Sera Sera"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })),
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    17,
                                    3.14,
                                    BinaryStringData.fromString("StarRocks"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                }),
                        generator.generate(
                                new Object[] {
                                    17,
                                    6.28,
                                    BinaryStringData.fromString("StarRocks"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })));
    }

    @Test
    void testValuesToStarRocks() throws Exception {
        TableId tableId =
                TableId.tableId(
                        StarRocksContainer.STARROCKS_DATABASE_NAME,
                        StarRocksContainer.STARROCKS_TABLE_NAME);
        DataStream<Event> stream =
                env.fromCollection(generateEvents(tableId), TypeInformation.of(Event.class));

        Configuration config =
                new Configuration()
                        .set(LOAD_URL, STARROCKS_CONTAINER.getLoadUrl())
                        .set(JDBC_URL, STARROCKS_CONTAINER.getJdbcUrl())
                        .set(USERNAME, StarRocksContainer.STARROCKS_USERNAME)
                        .set(PASSWORD, StarRocksContainer.STARROCKS_PASSWORD);

        Sink<Event> starRocksSink =
                ((FlinkSinkProvider) createStarRocksDataSink(config).getEventSinkProvider())
                        .getSink();
        stream.sinkTo(starRocksSink);

        env.execute("Values to StarRocks Sink");

        List<String> actual = fetchTableContent(tableId, 4);
        List<String> expected =
                Arrays.asList(
                        "17 | 6.28 | StarRocks | 2023-01-01 00:00:00.0",
                        "21 | 1.732 | Disenchanted | 2023-01-01 00:00:00.0");

        assertEqualsInAnyOrder(expected, actual);
    }

    @Test
    void testTimestampPrecisionToStarRocks() throws Exception {
        String precisionTable = "timestamp_precision_table";
        TableId tableId =
                TableId.tableId(StarRocksContainer.STARROCKS_DATABASE_NAME, precisionTable);

        executeSql(
                String.format(
                        "CREATE TABLE `%s`.`%s` ("
                                + "id INT NOT NULL, "
                                + "ts3 DATETIME, "
                                + "ts6 DATETIME"
                                + ") PRIMARY KEY (`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1"
                                + " PROPERTIES (\"replication_num\" = \"1\");",
                        StarRocksContainer.STARROCKS_DATABASE_NAME, precisionTable));

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT(), null))
                        .column(new PhysicalColumn("ts3", DataTypes.TIMESTAMP(3), null))
                        .column(new PhysicalColumn("ts6", DataTypes.TIMESTAMP(6), null))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(), DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6)));

        LocalDateTime dt3 = LocalDateTime.of(2026, 3, 27, 15, 20, 29, 123_000_000);
        LocalDateTime dt6 = LocalDateTime.of(2026, 3, 27, 15, 20, 29, 921_550_000);

        List<Event> events =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            1,
                                            TimestampData.fromLocalDateTime(dt3),
                                            TimestampData.fromLocalDateTime(dt6)
                                        })));

        DataStream<Event> stream =
                env.fromCollection(events, TypeInformation.of(Event.class));

        Configuration config =
                new Configuration()
                        .set(LOAD_URL, STARROCKS_CONTAINER.getLoadUrl())
                        .set(JDBC_URL, STARROCKS_CONTAINER.getJdbcUrl())
                        .set(USERNAME, StarRocksContainer.STARROCKS_USERNAME)
                        .set(PASSWORD, StarRocksContainer.STARROCKS_PASSWORD);

        Sink<Event> starRocksSink =
                ((FlinkSinkProvider) createStarRocksDataSink(config).getEventSinkProvider())
                        .getSink();
        stream.sinkTo(starRocksSink);

        env.execute("Timestamp Precision to StarRocks Sink");

        List<String> actual = fetchTableContent(tableId, 3);
        assertEqualsInAnyOrder(
                Collections.singletonList("1 | 2026-03-27 15:20:29.123 | 2026-03-27 15:20:29.92155"),
                actual);

        executeSql(
                String.format(
                        "DROP TABLE `%s`.`%s`;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME, precisionTable));
    }

    @Test
    void testLocalZonedTimestampPrecisionToStarRocks() throws Exception {
        String precisionTable = "ltz_precision_table";
        TableId tableId =
                TableId.tableId(StarRocksContainer.STARROCKS_DATABASE_NAME, precisionTable);

        executeSql(
                String.format(
                        "CREATE TABLE `%s`.`%s` ("
                                + "id INT NOT NULL, "
                                + "ts6 DATETIME"
                                + ") PRIMARY KEY (`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1"
                                + " PROPERTIES (\"replication_num\" = \"1\");",
                        StarRocksContainer.STARROCKS_DATABASE_NAME, precisionTable));

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT(), null))
                        .column(new PhysicalColumn("ts6", DataTypes.TIMESTAMP_LTZ(6), null))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.TIMESTAMP_LTZ(6)));

        // UTC 07:20:29.921550 -> stored as-is (pipeline local-time-zone is UTC in MockContext)
        Instant instant =
                LocalDateTime.of(2026, 3, 27, 7, 20, 29, 921_550_000).toInstant(ZoneOffset.UTC);

        List<Event> events =
                Arrays.asList(
                        new CreateTableEvent(tableId, schema),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            1, LocalZonedTimestampData.fromInstant(instant)
                                        })));

        DataStream<Event> stream =
                env.fromCollection(events, TypeInformation.of(Event.class));

        Configuration config =
                new Configuration()
                        .set(LOAD_URL, STARROCKS_CONTAINER.getLoadUrl())
                        .set(JDBC_URL, STARROCKS_CONTAINER.getJdbcUrl())
                        .set(USERNAME, StarRocksContainer.STARROCKS_USERNAME)
                        .set(PASSWORD, StarRocksContainer.STARROCKS_PASSWORD);

        Sink<Event> starRocksSink =
                ((FlinkSinkProvider) createStarRocksDataSink(config).getEventSinkProvider())
                        .getSink();
        stream.sinkTo(starRocksSink);

        env.execute("LocalZonedTimestamp Precision to StarRocks Sink");

        List<String> actual = fetchTableContent(tableId, 2);
        assertEqualsInAnyOrder(
                Collections.singletonList("1 | 2026-03-27 07:20:29.92155"), actual);

        executeSql(
                String.format(
                        "DROP TABLE `%s`.`%s`;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME, precisionTable));
    }
}
