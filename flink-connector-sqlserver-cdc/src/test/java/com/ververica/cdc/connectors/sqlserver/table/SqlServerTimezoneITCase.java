/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.sqlserver.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import com.ververica.cdc.connectors.sqlserver.SqlServerTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** Integration tests for SqlServer Table source. */
@RunWith(Parameterized.class)
public class SqlServerTimezoneITCase extends SqlServerTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @Parameterized.Parameter public String localTimeZone;

    @Parameterized.Parameters(name = "localTimeZone: {0}")
    public static List<String> parameters() {
        return Arrays.asList("Asia/Shanghai", "Europe/Berlin", "UTC");
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
    }

    @Test
    public void testTemporalTypesWithTimeZone() throws Exception {
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
                        localTimeZone);
        String sinkDDL =
                "CREATE TABLE sink (\n"
                        + "    id int NOT NULL,\n"
                        + "    val_date DATE,\n"
                        + "    val_time_p2 TIME(0),\n"
                        + "    val_time TIME(0),\n"
                        + "    val_datetime2 TIMESTAMP,\n"
                        + "    val_datetimeoffset TIMESTAMP_LTZ(3),\n"
                        + "    val_datetime TIMESTAMP,\n"
                        + "    val_smalldatetime TIMESTAMP,\n"
                        + "    PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM full_types");

        waitForSnapshotStarted("sink");

        List<String> expected = null;
        switch (localTimeZone) {
            case "Asia/Shanghai":
                expected =
                        Collections.singletonList(
                                "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T09:23:45.456Z, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
                break;
            case "Europe/Berlin":
                expected =
                        Collections.singletonList(
                                "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T03:23:45.456Z, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
                break;
            default:
                expected =
                        Collections.singletonList(
                                "+I[0, 2018-07-13, 10:23:45.680, 10:23:45.678, 2018-07-13T11:23:45.340, 2018-07-13T01:23:45.456Z, 2018-07-13T13:23:45.780, 2018-07-13T14:24]");
                break;
        }
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEquals(expected, actual);

        result.getJobClient().get().cancel().get();
    }
}
