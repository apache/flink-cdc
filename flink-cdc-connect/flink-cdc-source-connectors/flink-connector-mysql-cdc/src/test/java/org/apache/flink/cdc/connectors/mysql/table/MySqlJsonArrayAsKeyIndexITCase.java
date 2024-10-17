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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.flink.api.common.JobStatus.RUNNING;

/** Integration tests for MySQL Table source. */
@RunWith(Parameterized.class)
public class MySqlJsonArrayAsKeyIndexITCase extends MySqlSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlJsonArrayAsKeyIndexITCase.class);

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @Parameterized.Parameters(name = "incrementalSnapshot: {0}")
    public static Object[] parameters() {
        // MySQL 8.0.17 brought the `CAST(JSON_EXTRACT AS ARRAY)` syntax firstly, and originates the
        // "extra 0 byte" bug.
        // MySQL 8.0.18 changed the TYPED_ARRAY internal enum value from 244 to 20, but didn't fix
        // the bug.
        // MySQL 8.0.19 fixed this issue (eventually).
        return new Object[][] {
            new Object[] {MySqlVersion.V8_0_17},
            new Object[] {MySqlVersion.V8_0_18},
            new Object[] {MySqlVersion.V8_0_19}
        };
    }

    private final MySqlVersion version;
    private final MySqlContainer container;

    public MySqlJsonArrayAsKeyIndexITCase(MySqlVersion version) {
        this.version = version;
        this.container = createMySqlContainer(version, "docker/server-gtids/expire-seconds/my.cnf");
    }

    @Before
    public void before() {
        LOG.info("Starting MySQL {} containers...", version);
        Startables.deepStart(Stream.of(container)).join();
        LOG.info("Container MySQL {} is started.", version);
    }

    @After
    public void after() {
        LOG.info("Stopping MySQL {} containers...", version);
        container.stop();
        LOG.info("Container MySQL {} is stopped.", version);
    }

    @Test
    public void testJsonArrayAsKeyIndex() {
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
