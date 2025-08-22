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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.apache.flink.api.common.JobStatus.RUNNING;

/** Integration tests for MySQL Table source. */
public class MySqlJsonArrayAsKeyIndexITCase extends MySqlSourceTestBase {

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    private MySqlContainer container;

    @AfterEach
    public void after() {
        if (container != null) {
            container.stop();
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"V8_0_17", "V8_0_18", "V8_0_19"})
    public void testJsonArrayAsKeyIndex(MySqlVersion version) {
        this.container = createMySqlContainer(version, "docker/server-gtids/expire-seconds/my.cnf");
        Startables.deepStart(container).join();

        UniqueDatabase jaakiDatabase =
                new UniqueDatabase(container, "json_array_as_key", TEST_USER, TEST_PASSWORD);
        jaakiDatabase.createAndInitialize();

        String sourceDDL =
                String.format(
                        "CREATE TABLE json_array_as_key (\n"
                                + "    id BIGINT NOT NULL,\n"
                                + " PRIMARY KEY(id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'earliest-offset',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'server-id' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true'"
                                + ")",
                        container.getHost(),
                        container.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        jaakiDatabase.getDatabaseName(),
                        "json_array_as_key",
                        getServerId());
        tEnv.executeSql(sourceDDL);

        try (Connection connection = jaakiDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO json_array_as_key(id) VALUES (18),(19);");
            statement.execute("DELETE FROM json_array_as_key WHERE id=19;");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // async submit job
        TableResult result = tEnv.executeSql("SELECT * FROM json_array_as_key");

        try {
            // wait for the source startup, we don't have a better way to wait it, use sleep for
            // now
            do {
                Thread.sleep(5000L);
            } while (result.getJobClient().get().getJobStatus().get() != RUNNING);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        CloseableIterator<Row> iterator = result.collect();

        String[] expected =
                new String[] {
                    // snapshot records
                    "+I[17]", "+I[18]", "+I[19]", "-D[19]",
                };

        assertEqualsInAnyOrder(Arrays.asList(expected), fetchRows(iterator, expected.length));

        try {
            result.getJobClient().get().cancel().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + env.getParallelism());
    }
}
