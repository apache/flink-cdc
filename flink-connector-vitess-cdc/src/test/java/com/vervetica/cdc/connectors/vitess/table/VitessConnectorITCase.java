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

package com.vervetica.cdc.connectors.vitess.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.vervetica.cdc.connectors.vitess.VitessTestBase;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Integration tests for MySQL binlog SQL source. */
public class VitessConnectorITCase extends VitessTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
    }

    @Test
    public void testConsumingAllEvents()
            throws SQLException, ExecutionException, InterruptedException {
        initializeTable("inventory");
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'vitess-cdc',"
                                + " 'tablet-type' = 'MASTER',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'vtctl.hostname' = '%s',"
                                + " 'vtctl.port' = '%s',"
                                + " 'keyspace' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        VITESS_CONTAINER.getHost(),
                        VITESS_CONTAINER.getGrpcPort(),
                        VITESS_CONTAINER.getHost(),
                        VITESS_CONTAINER.getVtctldGrpcPort(),
                        VITESS_CONTAINER.getKeyspace(),
                        "test.products");
        String sinkDDL =
                "CREATE TABLE sink ("
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT name, SUM(weight) FROM debezium_source GROUP BY name");

        // Vitess source doesn't read snapshot data. Source will be empty at first.
        // There's no way knowing if it's started, using sleep here.
        Thread.sleep(10000);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO test.products \n"
                            + "VALUES (default,'scooter','Small 2-wheel scooter',3.14),\n"
                            + "       (default,'car battery','12V car battery',8.1),\n"
                            + "       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8),\n"
                            + "       (default,'hammer','12oz carpenters hammer',0.75),\n"
                            + "       (default,'hammer','14oz carpenters hammer',0.875),\n"
                            + "       (default,'hammer','16oz carpenters hammer',1.0),\n"
                            + "       (default,'rocks','box of assorted rocks',5.3),\n"
                            + "       (default,'jacket','water resistent black wind breaker',0.1),\n"
                            + "       (default,'spare tire','24 inch spare tire',22.2);");
            statement.execute(
                    "UPDATE test.products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE test.products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO test.products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO test.products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE test.products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE test.products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM test.products WHERE id=111;");
        }

        waitForSinkSize("sink", 20);

        List<String> expected =
                Arrays.asList(
                        "+I[scooter, 3.140]",
                        "+I[car battery, 8.100]",
                        "+I[12-pack drill bits, 0.800]",
                        "+I[hammer, 2.625]",
                        "+I[rocks, 5.100]",
                        "+I[jacket, 0.600]",
                        "+I[spare tire, 22.200]");

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertEqualsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testAllTypes() throws Throwable {
        initializeTable("column_type_test");
        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    `id` INT NOT NULL,\n"
                                + "    tiny_c TINYINT,\n"
                                + "    tiny_un_c SMALLINT ,\n"
                                + "    small_c SMALLINT,\n"
                                + "    small_un_c INT,\n"
                                + "    int_c INT ,\n"
                                + "    int_un_c BIGINT,\n"
                                + "    int11_c BIGINT,\n"
                                + "    big_c BIGINT,\n"
                                + "    varchar_c STRING,\n"
                                + "    char_c STRING,\n"
                                + "    float_c FLOAT,\n"
                                + "    double_c DOUBLE,\n"
                                + "    decimal_c DECIMAL(8, 4),\n"
                                + "    numeric_c DECIMAL(6, 0),\n"
                                + "    boolean_c BOOLEAN,\n"
                                + "    primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'vitess-cdc',"
                                + " 'tablet-type' = 'MASTER',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'vtctl.hostname' = '%s',"
                                + " 'vtctl.port' = '%s',"
                                + " 'keyspace' = '%s',"
                                + " 'table-name' = '%s'"
                                + ")",
                        VITESS_CONTAINER.getHost(),
                        VITESS_CONTAINER.getGrpcPort(),
                        VITESS_CONTAINER.getHost(),
                        VITESS_CONTAINER.getVtctldGrpcPort(),
                        VITESS_CONTAINER.getKeyspace(),
                        "test.full_types");
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM full_types");

        // Vitess source doesn't read snapshot data. Source will be empty at first.
        // There's no way knowing if it's started, using sleep here.
        Thread.sleep(10000);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO test.full_types VALUES (\n"
                            + "    DEFAULT, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807,\n"
                            + "    'Hello World', 'abc', 123.102, 404.4443, 123.4567, 345.6, true);");
            statement.execute("UPDATE test.full_types SET varchar_c = 'Bye World' WHERE id=1;");
        }

        waitForSnapshotStarted(result.collect());

        List<String> expected =
                Arrays.asList(
                        "+I[1, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807, Hello World, abc, 123.102, 404.4443, 123.4567, 346, true]",
                        "-U[1, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807, Hello World, abc, 123.102, 404.4443, 123.4567, 346, true]",
                        "+U[1, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807, Bye World, abc, 123.102, 404.4443, 123.4567, 346, true]");

        List<String> actual = fetchRows(result.collect(), expected.size());
        assertEquals(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    public static void assertEqualsInAnyOrder(List<String> actual, List<String> expected) {
        assertTrue(actual != null && expected != null);
        assertEquals(
                actual.stream().sorted().collect(Collectors.toList()),
                expected.stream().sorted().collect(Collectors.toList()));
    }

    private static void waitForSnapshotStarted(CloseableIterator<Row> iterator) throws Exception {
        while (!iterator.hasNext()) {
            Thread.sleep(100);
        }
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }
}
