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

package org.apache.flink.cdc.connectors.polardbx;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Stream;

/** Test supporting different column charsets for Polardbx. */
class PolardbxCharsetITCase extends PolardbxSourceTestBase {
    private static final String DATABASE = "charset_test";

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    public static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(
                        "utf8_test",
                        new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                        new String[] {
                            "-D[1, 测试数据]",
                            "-D[2, Craig Marshall]",
                            "-D[3, 另一个测试数据]",
                            "+I[11, 测试数据]",
                            "+I[12, Craig Marshall]",
                            "+I[13, 另一个测试数据]"
                        }),
                Arguments.of(
                        "ascii_test",
                        new String[] {
                            "+I[1, ascii test!?]", "+I[2, Craig Marshall]", "+I[3, {test}]"
                        },
                        new String[] {
                            "-D[1, ascii test!?]",
                            "-D[2, Craig Marshall]",
                            "-D[3, {test}]",
                            "+I[11, ascii test!?]",
                            "+I[12, Craig Marshall]",
                            "+I[13, {test}]"
                        }),
                Arguments.of(
                        "gbk_test",
                        new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                        new String[] {
                            "-D[1, 测试数据]",
                            "-D[2, Craig Marshall]",
                            "-D[3, 另一个测试数据]",
                            "+I[11, 测试数据]",
                            "+I[12, Craig Marshall]",
                            "+I[13, 另一个测试数据]"
                        }),
                Arguments.of(
                        "latin1_test",
                        new String[] {"+I[1, ÀÆÉ]", "+I[2, Craig Marshall]", "+I[3, Üæû]"},
                        new String[] {
                            "-D[1, ÀÆÉ]",
                            "-D[2, Craig Marshall]",
                            "-D[3, Üæû]",
                            "+I[11, ÀÆÉ]",
                            "+I[12, Craig Marshall]",
                            "+I[13, Üæû]"
                        }),
                Arguments.of(
                        "big5_test",
                        new String[] {"+I[1, 大五]", "+I[2, Craig Marshall]", "+I[3, 丹店]"},
                        new String[] {
                            "-D[1, 大五]",
                            "-D[2, Craig Marshall]",
                            "-D[3, 丹店]",
                            "+I[11, 大五]",
                            "+I[12, Craig Marshall]",
                            "+I[13, 丹店]"
                        }));
    }

    @BeforeAll
    public static void beforeClass() throws InterruptedException {
        initializePolardbxTables(
                DATABASE,
                s ->
                        !StringUtils.isNullOrWhitespaceOnly(s)
                                && (s.contains("utf8_test")
                                        || s.contains("latin1_test")
                                        || s.contains("gbk_test")
                                        || s.contains("big5_test")
                                        || s.contains("ascii_test")));
    }

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(200);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testCharset(String testName, String[] snapshotExpected, String[] binlogExpected)
            throws Exception {
        String sourceDDL =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  table_id BIGINT,\n"
                                + "  table_name STRING,\n"
                                + "  primary key(table_id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        testName,
                        getHost(),
                        getPort(),
                        USER_NAME,
                        PASSWORD,
                        DATABASE,
                        testName,
                        true,
                        getServerId(),
                        4);
        tEnv.executeSql(sourceDDL);
        // async submit job
        TableResult result =
                tEnv.executeSql(String.format("SELECT table_id,table_name FROM %s", testName));

        // test snapshot phase
        CloseableIterator<Row> iterator = result.collect();
        waitForSnapshotStarted(iterator);
        assertEqualsInAnyOrder(
                Arrays.asList(snapshotExpected), fetchRows(iterator, snapshotExpected.length));

        // test binlog phase
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "/*TDDL:FORBID_EXECUTE_DML_ALL=FALSE*/UPDATE %s.%s SET table_id = table_id + 10;",
                            DATABASE, testName));
        }
        assertEqualsInAnyOrder(
                Arrays.asList(binlogExpected), fetchRows(iterator, binlogExpected.length));
        result.getJobClient().get().cancel().get();
    }

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }
}
