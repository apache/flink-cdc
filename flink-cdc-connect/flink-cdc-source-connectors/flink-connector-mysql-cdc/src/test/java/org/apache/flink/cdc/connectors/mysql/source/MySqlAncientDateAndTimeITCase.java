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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Integration tests for MySQL Table source to handle ancient date and time records. */
public class MySqlAncientDateAndTimeITCase extends MySqlSourceTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlAncientDateAndTimeITCase.class);

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    // We need an extra "no_zero_in_date = false" config to insert malformed date and time records.
    private static final MySqlContainer MYSQL_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-allow-ancient-date-time/my.cnf");

    private final UniqueDatabase ancientDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "ancient_date_and_time", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeClass
    public static void beforeClass() {
        LOG.info("Starting MySql container...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Container MySql is started.");
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("Stopping MySql containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Container MySql is stopped.");
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        ancientDatabase.createAndInitialize();
    }

    @After
    public void after() {
        ancientDatabase.dropDatabase();
    }

    /**
     * With the TimeAdjuster in Debezium, all date / time records between year 0001 and 0099 will be
     * shifted to 1971 ~ 2069.
     */
    @Test
    public void testAncientDateAndTimeWithTimeAdjuster() throws Exception {
        // LocalDate.ofEpochDay reference:
        // +---------------------------------------------------------------------------------+
        // | 17390     | 11323    | 11720    | 23072    | -557266  | -1         | 18261      |
        // | 2017/8/12 | 2001/1/1 | 2002/2/2 | 2033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                true,
                Arrays.asList(
                        "+I[1, 17390, 2016-07-13T17:17:17, 2015-06-14T17:17:17.100, 2014-05-15T17:17:17.120, 2013-04-16T17:17:17.123, 2012-03-17T17:17:17.123400, 2011-02-18T17:17:17.123450, 2010-01-19T17:17:17.123456]",
                        "+I[2, null, null, null, null, null, null, null, null]",
                        "+I[3, 11323, 2001-01-01T16:16:16, 2001-01-01T16:16:16.100, 2001-01-01T16:16:16.120, 2001-01-01T16:16:16.123, 2001-01-01T16:16:16.123400, 2001-01-01T16:16:16.123450, 2001-01-01T16:16:16.123456]",
                        "+I[4, 11720, 2002-02-02T15:15:15, 2002-02-02T15:15:15.100, 2002-02-02T15:15:15.120, 2002-02-02T15:15:15.123, 2002-02-02T15:15:15.123400, 2002-02-02T15:15:15.123450, 2002-02-02T15:15:15.123456]",
                        "+I[5, 23072, 2033-03-03T14:14:14, 2033-03-03T14:14:14.100, 2033-03-03T14:14:14.120, 2033-03-03T14:14:14.123, 2033-03-03T14:14:14.123400, 2033-03-03T14:14:14.123450, 2033-03-03T14:14:14.123456]",
                        "+I[6, -557266, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "+I[7, -1, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "+I[8, 18261, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"));
    }

    @Test
    public void testAncientDateAndTimeWithoutTimeAdjuster() throws Exception {
        // LocalDate.ofEpochDay reference:
        // +---------------------------------------------------------------------------------+
        // | -713095   | -719162  | -718765  | -707413  | -557266  | -1         | 18261      |
        // | 0017/8/12 | 0001/1/1 | 0002/2/2 | 0033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                false,
                Arrays.asList(
                        "+I[1, -713095, 0016-07-13T17:17:17, 0015-06-14T17:17:17.100, 0014-05-15T17:17:17.120, 0013-04-16T17:17:17.123, 0012-03-17T17:17:17.123400, 0011-02-18T17:17:17.123450, 0010-01-19T17:17:17.123456]",
                        "+I[2, null, null, null, null, null, null, null, null]",
                        "+I[3, -719162, 0001-01-01T16:16:16, 0001-01-01T16:16:16.100, 0001-01-01T16:16:16.120, 0001-01-01T16:16:16.123, 0001-01-01T16:16:16.123400, 0001-01-01T16:16:16.123450, 0001-01-01T16:16:16.123456]",
                        "+I[4, -718765, 0002-02-02T15:15:15, 0002-02-02T15:15:15.100, 0002-02-02T15:15:15.120, 0002-02-02T15:15:15.123, 0002-02-02T15:15:15.123400, 0002-02-02T15:15:15.123450, 0002-02-02T15:15:15.123456]",
                        "+I[5, -707413, 0033-03-03T14:14:14, 0033-03-03T14:14:14.100, 0033-03-03T14:14:14.120, 0033-03-03T14:14:14.123, 0033-03-03T14:14:14.123400, 0033-03-03T14:14:14.123450, 0033-03-03T14:14:14.123456]",
                        "+I[6, -557266, 0444-04-04T13:13:13, 0444-04-04T13:13:13.100, 0444-04-04T13:13:13.120, 0444-04-04T13:13:13.123, 0444-04-04T13:13:13.123400, 0444-04-04T13:13:13.123450, 0444-04-04T13:13:13.123456]",
                        "+I[7, -1, 1969-12-31T12:12:12, 1969-12-31T12:12:12.100, 1969-12-31T12:12:12.120, 1969-12-31T12:12:12.123, 1969-12-31T12:12:12.123400, 1969-12-31T12:12:12.123450, 1969-12-31T12:12:12.123456]",
                        "+I[8, 18261, 2019-12-31T23:11:11, 2019-12-31T23:11:11.100, 2019-12-31T23:11:11.120, 2019-12-31T23:11:11.123, 2019-12-31T23:11:11.123400, 2019-12-31T23:11:11.123450, 2019-12-31T23:11:11.123456]"));
    }

    private void runGenericAncientDateAndTimeTest(
            MySqlContainer container,
            UniqueDatabase database,
            boolean enableTimeAdjuster,
            List<String> expectedResults)
            throws Exception {
        // Build deserializer
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("date_col", DataTypes.DATE()),
                        DataTypes.FIELD("datetime_0_col", DataTypes.TIMESTAMP(0)),
                        DataTypes.FIELD("datetime_1_col", DataTypes.TIMESTAMP(1)),
                        DataTypes.FIELD("datetime_2_col", DataTypes.TIMESTAMP(2)),
                        DataTypes.FIELD("datetime_3_col", DataTypes.TIMESTAMP(3)),
                        DataTypes.FIELD("datetime_4_col", DataTypes.TIMESTAMP(4)),
                        DataTypes.FIELD("datetime_5_col", DataTypes.TIMESTAMP(5)),
                        DataTypes.FIELD("datetime_6_col", DataTypes.TIMESTAMP(6)));
        LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);
        RowDataDebeziumDeserializeSchema deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType((RowType) dataType.getLogicalType())
                        .setResultTypeInfo(typeInfo)
                        .build();

        Properties dbzProperties = new Properties();
        dbzProperties.put("enable.time.adjuster", String.valueOf(enableTimeAdjuster));
        // Build source
        MySqlSource<RowData> mySqlSource =
                MySqlSource.<RowData>builder()
                        .hostname(container.getHost())
                        .port(container.getDatabasePort())
                        .databaseList(database.getDatabaseName())
                        .serverTimeZone("UTC")
                        .tableList(database.getDatabaseName() + ".ancient_times")
                        .username(database.getUsername())
                        .password(database.getPassword())
                        .serverId(getServerId())
                        .deserializer(deserializer)
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(dbzProperties)
                        .build();

        try (CloseableIterator<RowData> iterator =
                env.fromSource(
                                mySqlSource,
                                WatermarkStrategy.noWatermarks(),
                                "Backfill Skipped Source")
                        .executeAndCollect()) {
            Assertions.assertThat(fetchRows(iterator, expectedResults.size()))
                    .containsExactlyInAnyOrderElementsOf(expectedResults);
        }
    }

    private static List<String> fetchRows(Iterator<RowData> iter, int size) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return convertRowDataToRowString(rows);
    }

    private static List<String> convertRowDataToRowString(List<RowData> rows) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("id_col", 0);
        map.put("date_col", 1);
        map.put("datetime_0_col", 2);
        map.put("datetime_1_col", 3);
        map.put("datetime_2_col", 4);
        map.put("datetime_3_col", 5);
        map.put("datetime_4_col", 6);
        map.put("datetime_5_col", 7);
        map.put("datetime_6_col", 8);
        return rows.stream()
                .map(
                        row ->
                                RowUtils.createRowWithNamedPositions(
                                                row.getRowKind(),
                                                new Object[] {
                                                    wrap(row, 0, RowData::getInt),
                                                    wrap(row, 1, RowData::getInt),
                                                    wrap(row, 2, (r, i) -> r.getTimestamp(i, 0)),
                                                    wrap(row, 3, (r, i) -> r.getTimestamp(i, 1)),
                                                    wrap(row, 4, (r, i) -> r.getTimestamp(i, 2)),
                                                    wrap(row, 5, (r, i) -> r.getTimestamp(i, 3)),
                                                    wrap(row, 6, (r, i) -> r.getTimestamp(i, 4)),
                                                    wrap(row, 7, (r, i) -> r.getTimestamp(i, 5)),
                                                    wrap(row, 8, (r, i) -> r.getTimestamp(i, 6))
                                                },
                                                map)
                                        .toString())
                .collect(Collectors.toList());
    }

    private static <T> Object wrap(RowData row, int index, BiFunction<RowData, Integer, T> getter) {
        if (row.isNullAt(index)) {
            return null;
        }
        return getter.apply(row, index);
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + env.getParallelism());
    }
}
