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
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

    @BeforeAll
    public static void beforeClass() {
        LOG.info("Starting MySql container...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Container MySql is started.");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping MySql containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Container MySql is stopped.");
    }

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        ancientDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        ancientDatabase.dropDatabase();
    }

    /**
     * With the TimeAdjuster in Debezium, all date / time records between year 0001 and 0099 will be
     * shifted to 1971 ~ 2069.
     */
    @Test
    public void testAncientDateAndTimeWithTimeAdjusterWithRowDataDeserializer() throws Exception {
        // LocalDate.ofEpochDay reference:
        // +---------------------------------------------------------------------------------+
        // | 17390     | 11323    | 11720    | 23072    | -557266  | -1         | 18261      |
        // | 2017/8/12 | 2001/1/1 | 2002/2/2 | 2033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                true,
                DeserializerType.ROW_DATA,
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
    public void testAncientDateAndTimeWithoutTimeAdjusterWithRowDataDeserializer()
            throws Exception {
        // LocalDate.ofEpochDay reference:
        // +---------------------------------------------------------------------------------+
        // | -713095   | -719162  | -718765  | -707413  | -557266  | -1         | 18261      |
        // | 0017/8/12 | 0001/1/1 | 0002/2/2 | 0033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                false,
                DeserializerType.ROW_DATA,
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

    @Test
    public void testAncientDateAndTimeWithTimeAdjusterWithJsonDeserializer() throws Exception {
        // LocalDate.ofEpochDay reference:
        //
        // +---------------------------------------------------------------------------------+
        // | 17390     | 11323    | 11720    | 23072    | -557266  | -1         | 18261      |
        // | 2017/8/12 | 2001/1/1 | 2002/2/2 | 2033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        //
        // LocalDateTime.ofEpochSecond reference:
        //
        // Row 1:
        //    1468430237000 -> 2016-07-13T17:17:17
        //    1434302237100 -> 2015-06-14T17:17:17.100
        //    1400174237120 -> 2014-05-15T17:17:17.120
        //    1366132637123 -> 2013-04-16T17:17:17.123
        // 1332004637123400 -> 2012-03-17T17:17:17.123400
        // 1298049437123450 -> 2011-02-18T17:17:17.123450
        // 1263921437123456 -> 2010-01-19T17:17:17.123456
        //
        // Row 2:
        //   (null)
        //
        // Row 3:
        //    978365776000 -> 2001-01-01T16:16:16
        //    978365776100 -> 2001-01-01T16:16:16.100
        //    978365776120 -> 2001-01-01T16:16:16.120
        //    978365776123 -> 2001-01-01T16:16:16.123
        // 978365776123400 -> 2001-01-01T16:16:16.123400
        // 978365776123450 -> 2001-01-01T16:16:16.123450
        // 978365776123456 -> 2001-01-01T16:16:16.123456
        //
        // Row 4:
        //    1012662915000 -> 2002-02-02T15:15:15
        //    1012662915100 -> 2002-02-02T15:15:15.100
        //    1012662915120 -> 2002-02-02T15:15:15.120
        //    1012662915123 -> 2002-02-02T15:15:15.123
        // 1012662915123400 -> 2002-02-02T15:15:15.123400
        // 1012662915123450 -> 2002-02-02T15:15:15.123450
        // 1012662915123456 -> 2002-02-02T15:15:15.123456
        //
        // Row 5:
        //    1993472054000 -> 2033-03-03T14:14:14
        //    1993472054100 -> 2033-03-03T14:14:14.100
        //    1993472054120 -> 2033-03-03T14:14:14.120
        //    1993472054123 -> 2033-03-03T14:14:14.123
        // 1993472054123400 -> 2033-03-03T14:14:14.123400
        // 1993472054123450 -> 2033-03-03T14:14:14.123450
        // 1993472054123456 -> 2033-03-03T14:14:14.123456
        //
        // Row 6:
        //    -48147734807000 -> 0444-04-04T13:13:13
        //    -48147734806900 -> 0444-04-04T13:13:13.100
        //    -48147734806880 -> 0444-04-04T13:13:13.120
        //    -48147734806877 -> 0444-04-04T13:13:13.123
        // -48147734806876600 -> 0444-04-04T13:13:13.000123400
        // -48147734806876550 -> 0444-04-04T13:13:13.000123450
        // -48147734806876544 -> 0444-04-04T13:13:13.000123456
        //
        // Row 7:
        //    -42468000 -> 1969-12-31T12:12:12
        //    -42467900 -> 1969-12-31T12:12:12.100
        //    -42467880 -> 1969-12-31T12:12:12.120
        //    -42467877 -> 1969-12-31T12:12:12.123
        // -42467876600 -> 1969-12-31T12:12:12.123400
        // -42467876550 -> 1969-12-31T12:12:12.123450
        // -42467876544 -> 1969-12-31T12:12:12.123456
        //
        // Row 8:
        //    1577833871000 -> 2019-12-31T23:11:11
        //    1577833871100 -> 2019-12-31T23:11:11.100
        //    1577833871120 -> 2019-12-31T23:11:11.120
        //    1577833871123 -> 2019-12-31T23:11:11.123
        // 1577833871123400 -> 2019-12-31T23:11:11.123400
        // 1577833871123450 -> 2019-12-31T23:11:11.123450
        // 1577833871123456 -> 2019-12-31T23:11:11.123456

        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                true,
                DeserializerType.JSON,
                Arrays.asList(
                        "{\"id\":\"AQ==\",\"date_col\":17390,\"datetime_0_col\":1468430237000,\"datetime_1_col\":1434302237100,\"datetime_2_col\":1400174237120,\"datetime_3_col\":1366132637123,\"datetime_4_col\":1332004637123400,\"datetime_5_col\":1298049437123450,\"datetime_6_col\":1263921437123456}",
                        "{\"id\":\"Ag==\",\"date_col\":null,\"datetime_0_col\":null,\"datetime_1_col\":null,\"datetime_2_col\":null,\"datetime_3_col\":null,\"datetime_4_col\":null,\"datetime_5_col\":null,\"datetime_6_col\":null}",
                        "{\"id\":\"Aw==\",\"date_col\":11323,\"datetime_0_col\":978365776000,\"datetime_1_col\":978365776100,\"datetime_2_col\":978365776120,\"datetime_3_col\":978365776123,\"datetime_4_col\":978365776123400,\"datetime_5_col\":978365776123450,\"datetime_6_col\":978365776123456}",
                        "{\"id\":\"BA==\",\"date_col\":11720,\"datetime_0_col\":1012662915000,\"datetime_1_col\":1012662915100,\"datetime_2_col\":1012662915120,\"datetime_3_col\":1012662915123,\"datetime_4_col\":1012662915123400,\"datetime_5_col\":1012662915123450,\"datetime_6_col\":1012662915123456}",
                        "{\"id\":\"BQ==\",\"date_col\":23072,\"datetime_0_col\":1993472054000,\"datetime_1_col\":1993472054100,\"datetime_2_col\":1993472054120,\"datetime_3_col\":1993472054123,\"datetime_4_col\":1993472054123400,\"datetime_5_col\":1993472054123450,\"datetime_6_col\":1993472054123456}",
                        "{\"id\":\"Bg==\",\"date_col\":-557266,\"datetime_0_col\":-48147734807000,\"datetime_1_col\":-48147734806900,\"datetime_2_col\":-48147734806880,\"datetime_3_col\":-48147734806877,\"datetime_4_col\":-48147734806876600,\"datetime_5_col\":-48147734806876550,\"datetime_6_col\":-48147734806876544}",
                        "{\"id\":\"Bw==\",\"date_col\":-1,\"datetime_0_col\":-42468000,\"datetime_1_col\":-42467900,\"datetime_2_col\":-42467880,\"datetime_3_col\":-42467877,\"datetime_4_col\":-42467876600,\"datetime_5_col\":-42467876550,\"datetime_6_col\":-42467876544}",
                        "{\"id\":\"CA==\",\"date_col\":18261,\"datetime_0_col\":1577833871000,\"datetime_1_col\":1577833871100,\"datetime_2_col\":1577833871120,\"datetime_3_col\":1577833871123,\"datetime_4_col\":1577833871123400,\"datetime_5_col\":1577833871123450,\"datetime_6_col\":1577833871123456}"));
    }

    @Test
    public void testAncientDateAndTimeWithoutTimeAdjusterWithJsonDeserializer() throws Exception {
        // LocalDate.ofEpochDay reference:
        //
        // +---------------------------------------------------------------------------------+
        // | -713095   | -719162  | -718765  | -707413  | -557266  | -1         | 18261      |
        // | 0017/8/12 | 0001/1/1 | 0002/2/2 | 0033/3/3 | 0444/4/4 | 1969/12/31 | 2019/12/31 |
        // +---------------------------------------------------------------------------------+
        //
        // LocalDateTime.ofEpochSecond reference:
        //
        // Row 1:
        //    -61645473763000 -> 0016-07-13T17:17:17
        //    -61679601762900 -> 0015-06-14T17:17:17.100
        //    -61713729762880 -> 0014-05-15T17:17:17.120
        //    -61747771362877 -> 0013-04-16T17:17:17.123
        // -61781899362876600 -> 0012-03-17T17:17:17.123400
        // -61815854562876550 -> 0011-02-18T17:17:17.123450
        // -61849982562876544 -> 0010-01-19T17:17:17.123456
        //
        // Row 2:
        //   (null)
        //
        // Row 3:
        //    -62135538224000 -> 0001-01-01T16:16:16
        //    -62135538223900 -> 0001-01-01T16:16:16.100
        //    -62135538223880 -> 0001-01-01T16:16:16.120
        //    -62135538223877 -> 0001-01-01T16:16:16.123
        // -62135538223876600 -> 0001-01-01T16:16:16.123400
        // -62135538223876550 -> 0001-01-01T16:16:16.123450
        // -62135538223876544 -> 0001-01-01T16:16:16.123456
        //
        // Row 4:
        //    -62101241085000 -> 0002-02-02T15:15:15
        //    -62101241084900 -> 0002-02-02T15:15:15.100
        //    -62101241084880 -> 0002-02-02T15:15:15.120
        //    -62101241084877 -> 0002-02-02T15:15:15.123
        // -62101241084876600 -> 0002-02-02T15:15:15.123400
        // -62101241084876550 -> 0002-02-02T15:15:15.123450
        // -62101241084876544 -> 0002-02-02T15:15:15.123456
        //
        // Row 5:
        //    -61120431946000 -> 0033-03-03T14:14:14
        //    -61120431945900 -> 0033-03-03T14:14:14.100
        //    -61120431945880 -> 0033-03-03T14:14:14.120
        //    -61120431945877 -> 0033-03-03T14:14:14.123
        // -61120431945876600 -> 0033-03-03T14:14:14.123400
        // -61120431945876550 -> 0033-03-03T14:14:14.123450
        // -61120431945876544 -> 0033-03-03T14:14:14.123456
        //
        //
        // Row 6:
        //    -48147734807000 -> 0444-04-04T13:13:13
        //    -48147734806900 -> 0444-04-04T13:13:13.100
        //    -48147734806880 -> 0444-04-04T13:13:13.120
        //    -48147734806877 -> 0444-04-04T13:13:13.123
        // -48147734806876600 -> 0444-04-04T13:13:13.000123400
        // -48147734806876550 -> 0444-04-04T13:13:13.000123450
        // -48147734806876544 -> 0444-04-04T13:13:13.000123456
        //
        // Row 7:
        //    -42468000 -> 1969-12-31T12:12:12
        //    -42467900 -> 1969-12-31T12:12:12.100
        //    -42467880 -> 1969-12-31T12:12:12.120
        //    -42467877 -> 1969-12-31T12:12:12.123
        // -42467876600 -> 1969-12-31T12:12:12.123400
        // -42467876550 -> 1969-12-31T12:12:12.123450
        // -42467876544 -> 1969-12-31T12:12:12.123456
        //
        // Row 8:
        //    1577833871000 -> 2019-12-31T23:11:11
        //    1577833871100 -> 2019-12-31T23:11:11.100
        //    1577833871120 -> 2019-12-31T23:11:11.120
        //    1577833871123 -> 2019-12-31T23:11:11.123
        // 1577833871123400 -> 2019-12-31T23:11:11.123400
        // 1577833871123450 -> 2019-12-31T23:11:11.123450
        // 1577833871123456 -> 2019-12-31T23:11:11.123456

        runGenericAncientDateAndTimeTest(
                MYSQL_CONTAINER,
                ancientDatabase,
                false,
                DeserializerType.JSON,
                Arrays.asList(
                        "{\"id\":\"AQ==\",\"date_col\":-713095,\"datetime_0_col\":-61645473763000,\"datetime_1_col\":-61679601762900,\"datetime_2_col\":-61713729762880,\"datetime_3_col\":-61747771362877,\"datetime_4_col\":-61781899362876600,\"datetime_5_col\":-61815854562876550,\"datetime_6_col\":-61849982562876544}",
                        "{\"id\":\"Ag==\",\"date_col\":null,\"datetime_0_col\":null,\"datetime_1_col\":null,\"datetime_2_col\":null,\"datetime_3_col\":null,\"datetime_4_col\":null,\"datetime_5_col\":null,\"datetime_6_col\":null}",
                        "{\"id\":\"Aw==\",\"date_col\":-719162,\"datetime_0_col\":-62135538224000,\"datetime_1_col\":-62135538223900,\"datetime_2_col\":-62135538223880,\"datetime_3_col\":-62135538223877,\"datetime_4_col\":-62135538223876600,\"datetime_5_col\":-62135538223876550,\"datetime_6_col\":-62135538223876544}",
                        "{\"id\":\"BA==\",\"date_col\":-718765,\"datetime_0_col\":-62101241085000,\"datetime_1_col\":-62101241084900,\"datetime_2_col\":-62101241084880,\"datetime_3_col\":-62101241084877,\"datetime_4_col\":-62101241084876600,\"datetime_5_col\":-62101241084876550,\"datetime_6_col\":-62101241084876544}",
                        "{\"id\":\"BQ==\",\"date_col\":-707413,\"datetime_0_col\":-61120431946000,\"datetime_1_col\":-61120431945900,\"datetime_2_col\":-61120431945880,\"datetime_3_col\":-61120431945877,\"datetime_4_col\":-61120431945876600,\"datetime_5_col\":-61120431945876550,\"datetime_6_col\":-61120431945876544}",
                        "{\"id\":\"Bg==\",\"date_col\":-557266,\"datetime_0_col\":-48147734807000,\"datetime_1_col\":-48147734806900,\"datetime_2_col\":-48147734806880,\"datetime_3_col\":-48147734806877,\"datetime_4_col\":-48147734806876600,\"datetime_5_col\":-48147734806876550,\"datetime_6_col\":-48147734806876544}",
                        "{\"id\":\"Bw==\",\"date_col\":-1,\"datetime_0_col\":-42468000,\"datetime_1_col\":-42467900,\"datetime_2_col\":-42467880,\"datetime_3_col\":-42467877,\"datetime_4_col\":-42467876600,\"datetime_5_col\":-42467876550,\"datetime_6_col\":-42467876544}",
                        "{\"id\":\"CA==\",\"date_col\":18261,\"datetime_0_col\":1577833871000,\"datetime_1_col\":1577833871100,\"datetime_2_col\":1577833871120,\"datetime_3_col\":1577833871123,\"datetime_4_col\":1577833871123400,\"datetime_5_col\":1577833871123450,\"datetime_6_col\":1577833871123456}"));
    }

    private void runGenericAncientDateAndTimeTest(
            MySqlContainer container,
            UniqueDatabase database,
            boolean enableTimeAdjuster,
            DeserializerType deserializerType,
            List<String> expectedResults)
            throws Exception {

        switch (deserializerType) {
            case ROW_DATA:
                {
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
                                            "Fetch results")
                                    .executeAndCollect()) {
                        List<RowData> results = fetchRows(iterator, expectedResults.size());
                        Assertions.assertThat(convertRowDataToRowString(results))
                                .containsExactlyInAnyOrderElementsOf(expectedResults);
                    }
                }
                break;
            case JSON:
                {
                    JsonDebeziumDeserializationSchema deserializer =
                            new JsonDebeziumDeserializationSchema();

                    Properties dbzProperties = new Properties();
                    dbzProperties.put("enable.time.adjuster", String.valueOf(enableTimeAdjuster));
                    // Build source
                    MySqlSource<String> mySqlSource =
                            MySqlSource.<String>builder()
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

                    try (CloseableIterator<String> iterator =
                            env.fromSource(
                                            mySqlSource,
                                            WatermarkStrategy.noWatermarks(),
                                            "Fetch results")
                                    .executeAndCollect()) {
                        List<String> results = fetchRows(iterator, expectedResults.size());
                        Assertions.assertThat(convertJsonToRowString(results))
                                .containsExactlyInAnyOrderElementsOf(expectedResults);
                    }
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown deserializer type: " + deserializerType);
        }
    }

    private static <T> List<T> fetchRows(Iterator<T> iter, int size) {
        List<T> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            T row = iter.next();
            rows.add(row);
            size--;
        }
        return rows;
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

    private static List<String> convertJsonToRowString(List<String> rows) {
        ObjectMapper mapper = new ObjectMapper();
        return rows.stream()
                .map(
                        row -> {
                            try {
                                JsonNode node = mapper.readTree(row);
                                return node.get("after").toString();
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        })
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

    enum DeserializerType {
        JSON,
        ROW_DATA
    }
}
