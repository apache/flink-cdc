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

package org.apache.flink.cdc.connectors.sqlserver.table;

import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Integration tests for SqlServer Table source. */
class SqlServerTimezoneITCase extends SqlServerTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Asia/Shanghai", "Europe/Berlin", "UTC"})
    void testTimeTypeWithDifferentLocalTimeZones(String timeZone) throws Exception {
        List<String> actual = getTimestampResult(timeZone, "UTC", "UTC", false);
        // timestamp_ltz is not determined by timezones, different local timezone with same value.
        List<String> expected =
                Collections.singletonList(
                        "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T01:23:45.456Z, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Asia/Shanghai", "Europe/Berlin", "UTC"})
    void testTimeTypeWithDifferentServerTimeZones(String timeZone) throws Exception {
        List<String> actual = getTimestampResult("UTC", timeZone, "UTC", false);
        // timestamp_ltz is not determined by timezones, different server timeZone with same value.
        List<String> expected =
                Collections.singletonList(
                        "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T01:23:45.456Z, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Asia/Shanghai", "Europe/Berlin", "UTC"})
    void testTimeTypeWithDifferentJVMTimeZones(String timeZone) throws Exception {
        List<String> actual = getTimestampResult("UTC", "UTC", timeZone, false);
        // timestamp_ltz is not determined by timezones, different jvm timezone with same value.
        List<String> expected =
                Collections.singletonList(
                        "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T01:23:45.456Z, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Asia/Shanghai", "Europe/Berlin", "UTC"})
    void testTimeTypeLtz2ntz(String timeZone) throws Exception {
        List<String> actual = getTimestampResult(timeZone, "UTC", "UTC", true);

        // A timestamp value from a same timestamp_ltz value is determined by flink timezone,
        // different local timezone with different value.
        List<String> expected = null;
        switch (timeZone) {
            case "Asia/Shanghai":
                expected =
                        Collections.singletonList(
                                "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T09:23:45.456, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
                break;
            case "Europe/Berlin":
                expected =
                        Collections.singletonList(
                                "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T03:23:45.456, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
                break;
            default:
                expected =
                        Collections.singletonList(
                                "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T01:23:45.456, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
                break;
        }
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    public List<String> getTimestampResult(
            String localTimeZone,
            String serverTimeZone,
            String jvmTimeZone,
            boolean castTimeStampLtz)
            throws InterruptedException, ExecutionException {

        TimeZone aDefault = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(jvmTimeZone));
            initializeSqlServerTable("column_type_test");

            String sourceDDL =
                    String.format(
                            "CREATE TABLE full_types (\n"
                                    + "    id int NOT NULL,\n"
                                    + "    val_date DATE,\n"
                                    + "    val_time_p2 TIME(0),\n"
                                    + "    val_time TIME(0),\n"
                                    + "    val_datetime2 TIMESTAMP,\n"
                                    + "    val_datetimeoffset TIMESTAMP_LTZ(3),\n"
                                    + "    val_datetime TIMESTAMP,\n"
                                    + "    val_smalldatetime TIMESTAMP\n"
                                    + ") WITH ("
                                    + " 'connector' = 'sqlserver-cdc',"
                                    + " 'hostname' = '%s',"
                                    + " 'port' = '%s',"
                                    + " 'username' = '%s',"
                                    + " 'password' = '%s',"
                                    + " 'database-name' = '%s',"
                                    + " 'table-name' = '%s',"
                                    + " 'server-time-zone'='%s'"
                                    + ")",
                            MSSQL_SERVER_CONTAINER.getHost(),
                            MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT),
                            MSSQL_SERVER_CONTAINER.getUsername(),
                            MSSQL_SERVER_CONTAINER.getPassword(),
                            "column_type_test",
                            "dbo.full_types",
                            serverTimeZone);
            String sinkDDL =
                    "CREATE TABLE sink (\n"
                            + "    id int NOT NULL,\n"
                            + "    val_date DATE,\n"
                            + "    val_time_p2 TIME(0),\n"
                            + "    val_time TIME(0),\n"
                            + "    val_datetime2 TIMESTAMP,\n"
                            + (castTimeStampLtz
                                    ? "    val_datetimeoffset TIMESTAMP(3),\n"
                                    : "    val_datetimeoffset TIMESTAMP_LTZ(3),\n")
                            + "    val_datetime TIMESTAMP,\n"
                            + "    val_smalldatetime TIMESTAMP,\n"
                            + "    PRIMARY KEY (id) NOT ENFORCED"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false',"
                            + " 'sink-expected-messages-num' = '20'"
                            + ")";
            // set table.local-time-zone to serverTimeZone
            tEnv.getConfig().setLocalTimeZone(ZoneId.of(localTimeZone));
            tEnv.executeSql(sourceDDL);
            tEnv.executeSql(sinkDDL);

            // async submit job
            TableResult result = tEnv.executeSql("INSERT INTO sink SELECT *  FROM full_types");

            waitForSnapshotStarted("sink");

            List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("sink");

            result.getJobClient().get().cancel().get();
            return actual;
        } finally {
            TimeZone.setDefault(aDefault);
        }
    }
}
