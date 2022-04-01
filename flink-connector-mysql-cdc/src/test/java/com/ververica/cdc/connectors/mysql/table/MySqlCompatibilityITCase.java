/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.connectors.mysql.MySqlValidatorTest;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.MySqlVersion;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase.assertEqualsInAnyOrder;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase.assertEqualsInOrder;

/** Integration tests to check mysql-cdc works well with different MySQL server version. */
public class MySqlCompatibilityITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCompatibilityITCase.class);

    private static TemporaryFolder tempFolder;
    private static File resourceFolder;

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

    @Before
    public void setup() throws Exception {
        resourceFolder =
                Paths.get(
                                Objects.requireNonNull(
                                                MySqlValidatorTest.class
                                                        .getClassLoader()
                                                        .getResource("."))
                                        .toURI())
                        .toFile();
        tempFolder = new TemporaryFolder(resourceFolder);
        tempFolder.create();
        env.setParallelism(4);
        env.enableCheckpointing(200);
    }

    @Test
    public void testMySqlV56() throws Exception {
        testDifferentMySqlVersion(MySqlVersion.V5_6, false);
    }

    @Test
    public void testMySqlV56WithGtidModeOn() throws Exception {
        testDifferentMySqlVersion(MySqlVersion.V5_6, true);
    }

    @Test
    public void testMySqlV57() throws Exception {
        testDifferentMySqlVersion(MySqlVersion.V5_7, false);
    }

    @Test
    public void testMySqlV57WithGtidModeOn() throws Exception {
        testDifferentMySqlVersion(MySqlVersion.V5_7, true);
    }

    @Test
    public void testMySqlV8() throws Exception {
        testDifferentMySqlVersion(MySqlVersion.V8_0, false);
    }

    @Test
    public void testMySqlV8WithGtidModeOn() throws Exception {
        testDifferentMySqlVersion(MySqlVersion.V8_0, true);
    }

    private void testDifferentMySqlVersion(MySqlVersion version, boolean enableGtid)
            throws Exception {
        final MySqlContainer mySqlContainer =
                (MySqlContainer)
                        new MySqlContainer(version)
                                .withConfigurationOverride(
                                        buildCustomMySqlConfig(version, enableGtid))
                                .withSetupSQL("docker/setup.sql")
                                .withDatabaseName("flink-test")
                                .withUsername("flinkuser")
                                .withPassword("flinkpw")
                                .withLogConsumer(new Slf4jLogConsumer(LOG));

        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(mySqlContainer)).join();
        LOG.info("Containers are started.");

        UniqueDatabase testDatabase =
                new UniqueDatabase(mySqlContainer, "inventory", "mysqluser", "mysqlpw");
        testDatabase.createAndInitialize();

        String sourceDDL =
                String.format(
                        "CREATE TABLE products ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'server-id' = '%s'"
                                + ")",
                        mySqlContainer.getHost(),
                        mySqlContainer.getDatabasePort(),
                        testDatabase.getUsername(),
                        testDatabase.getPassword(),
                        testDatabase.getDatabaseName(),
                        "products",
                        getServerId());
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql("SELECT `id`, name, description, weight FROM products");

        CloseableIterator<Row> iterator = result.collect();

        String[] expectedSnapshot =
                new String[] {
                    "+I[101, scooter, Small 2-wheel scooter, 3.140]",
                    "+I[102, car battery, 12V car battery, 8.100]",
                    "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                    "+I[104, hammer, 12oz carpenter's hammer, 0.750]",
                    "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                    "+I[106, hammer, 16oz carpenter's hammer, 1.000]",
                    "+I[107, rocks, box of assorted rocks, 5.300]",
                    "+I[108, jacket, water resistent black wind breaker, 0.100]",
                    "+I[109, spare tire, 24 inch spare tire, 22.200]"
                };
        assertEqualsInAnyOrder(
                Arrays.asList(expectedSnapshot), fetchRows(iterator, expectedSnapshot.length));

        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }

        String[] expectedBinlog =
                new String[] {
                    "-U[106, hammer, 16oz carpenter's hammer, 1.000]",
                    "+U[106, hammer, 18oz carpenter hammer, 1.000]",
                    "-U[107, rocks, box of assorted rocks, 5.300]",
                    "+U[107, rocks, box of assorted rocks, 5.100]",
                    "+I[110, jacket, water resistent white wind breaker, 0.200]",
                    "+I[111, scooter, Big 2-wheel scooter , 5.180]",
                    "-U[110, jacket, water resistent white wind breaker, 0.200]",
                    "+U[110, jacket, new water resistent white wind breaker, 0.500]",
                    "-U[111, scooter, Big 2-wheel scooter , 5.180]",
                    "+U[111, scooter, Big 2-wheel scooter , 5.170]",
                    "-D[111, scooter, Big 2-wheel scooter , 5.170]"
                };

        assertEqualsInOrder(
                Arrays.asList(expectedBinlog), fetchRows(iterator, expectedBinlog.length));
        result.getJobClient().get().cancel().get();
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + env.getParallelism());
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

    private String buildCustomMySqlConfig(MySqlVersion version, boolean enableGtid) {
        try {
            File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
            Path cnf = Files.createFile(Paths.get(folder.getPath(), "my.cnf"));
            StringBuilder mysqlConfBuilder = new StringBuilder();
            mysqlConfBuilder.append(
                    "[mysqld]\n"
                            + "binlog_format = row\n"
                            + "log_bin = mysql-bin\n"
                            + "server-id = 223344\n"
                            + "binlog_row_image = FULL\n");
            if (!enableGtid) {
                mysqlConfBuilder.append("gtid-mode = OFF\n");
            } else {
                mysqlConfBuilder.append("gtid-mode = ON\n");
                mysqlConfBuilder.append("enforce-gtid-consistency = 1\n");
                // see
                // https://dev.mysql.com/doc/refman/5.7/en/replication-options-gtids.html#sysvar_gtid_mode
                if (version == MySqlVersion.V5_6 || version == MySqlVersion.V5_7) {
                    mysqlConfBuilder.append("log-slave-updates = ON\n");
                }
            }

            if (version == MySqlVersion.V8_0) {
                mysqlConfBuilder.append("secure_file_priv=/var/lib/mysql\n");
            }

            Files.write(
                    cnf,
                    Collections.singleton(mysqlConfBuilder.toString()),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return Paths.get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }
}
