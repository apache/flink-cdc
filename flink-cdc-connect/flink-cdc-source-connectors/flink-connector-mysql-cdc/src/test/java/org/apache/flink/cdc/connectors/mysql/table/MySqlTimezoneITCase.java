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

import org.apache.flink.cdc.connectors.mysql.MySqlValidatorTest;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase.assertEqualsInAnyOrder;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase.assertEqualsInOrder;

/** Integration tests to check mysql-cdc works well under different MySQL server timezone. */
class MySqlTimezoneITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlTimezoneITCase.class);
    private Path tempFolder;
    private static File resourceFolder;
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    public void setup(boolean incrementalSnapshot) throws Exception {
        resourceFolder =
                Paths.get(
                                Objects.requireNonNull(
                                                MySqlValidatorTest.class
                                                        .getClassLoader()
                                                        .getResource("."))
                                        .toURI())
                        .toFile();
        if (incrementalSnapshot) {
            env.setParallelism(4);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
        tempFolder = Files.createTempDirectory(resourceFolder.toPath(), "mysql-config");
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMySqlServerInBerlin(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        testTemporalTypesWithMySqlServerTimezone("Europe/Berlin", incrementalSnapshot);
    }

    @ParameterizedTest(name = "incrementalSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMySqlServerInShanghai(boolean incrementalSnapshot) throws Exception {
        setup(incrementalSnapshot);
        testTemporalTypesWithMySqlServerTimezone("Asia/Shanghai", incrementalSnapshot);
    }

    private void testTemporalTypesWithMySqlServerTimezone(
            String timezone, boolean incrementalSnapshot) throws Exception {
        MySqlContainer mySqlContainer =
                (MySqlContainer)
                        new MySqlContainer()
                                .withConfigurationOverride(buildMySqlConfigWithTimezone(timezone))
                                .withSetupSQL("docker/setup.sql")
                                .withDatabaseName("flink-test")
                                .withUsername("flinkuser")
                                .withPassword("flinkpw")
                                .withLogConsumer(new Slf4jLogConsumer(LOG));

        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(mySqlContainer)).join();
        LOG.info("Containers are started.");

        UniqueDatabase fullTypesDatabase =
                new UniqueDatabase(mySqlContainer, "column_type_test", "mysqluser", "mysqlpw");
        fullTypesDatabase.createAndInitialize();

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
                                + "    date_c DATE,\n"
                                + "    time_c TIME(0),\n"
                                + "    datetime3_c TIMESTAMP(3),\n"
                                + "    datetime6_c TIMESTAMP(6),\n"
                                + "    timestamp_c TIMESTAMP(0),\n"
                                + "    file_uuid BYTES,\n"
                                + "    primary key (`id`) not enforced"
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
                                + " 'scan.incremental.snapshot.chunk.size' = '%s',"
                                + " 'server-time-zone'='%s'"
                                + ")",
                        mySqlContainer.getHost(),
                        mySqlContainer.getDatabasePort(),
                        fullTypesDatabase.getUsername(),
                        fullTypesDatabase.getPassword(),
                        fullTypesDatabase.getDatabaseName(),
                        "full_types",
                        incrementalSnapshot,
                        getServerId(incrementalSnapshot),
                        getSplitSize(incrementalSnapshot),
                        timezone);
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "SELECT date_c, time_c, datetime3_c, datetime6_c, timestamp_c FROM full_types");

        CloseableIterator<Row> iterator = result.collect();
        String[] expectedSnapshot =
                new String[] {
                    "+I[2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22]"
                };
        assertEqualsInAnyOrder(
                Arrays.asList(expectedSnapshot), fetchRows(iterator, expectedSnapshot.length));

        try (Connection connection = fullTypesDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        String[] expectedBinlog =
                new String[] {
                    "-U[2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22]",
                    "+U[2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:33:22]"
                };

        assertEqualsInOrder(
                Arrays.asList(expectedBinlog), fetchRows(iterator, expectedBinlog.length));

        result.getJobClient().get().cancel().get();
        mySqlContainer.stop();
    }

    private String getServerId(boolean incrementalSnapshot) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        if (incrementalSnapshot) {
            return serverId + "-" + (serverId + env.getParallelism());
        }
        return String.valueOf(serverId);
    }

    private int getSplitSize(boolean incrementalSnapshot) {
        if (incrementalSnapshot) {
            // test parallel read
            return 4;
        }
        return 0;
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

    private String buildMySqlConfigWithTimezone(String timezone) {
        // JVM timezone is in "GMT+XX:XX" or "GMT-XX:XX" format
        // while MySQL configuration file requires "+XX:XX" or "-XX:XX"
        if (timezone.startsWith("GMT")) {
            timezone = timezone.substring(3);
        }

        // But if we run JVM with -Duser.timezone=GMT+0:00, the timezone String will be set to "GMT"
        // (without redundant offset part). We can't pass an empty string to MySQL, or it will
        // panic.
        if (timezone.isEmpty()) {
            timezone = "UTC";
        }

        try {
            Path cnf = Files.createFile(Paths.get(tempFolder.toString(), "my.cnf"));
            String mysqldConf =
                    "[mysqld]\n"
                            + "binlog_format = row\n"
                            + "log_bin = mysql-bin\n"
                            + "server-id = 223344\n"
                            + "binlog_row_image = FULL\n";
            String timezoneConf = "default-time_zone = '" + timezone + "'\n";
            Files.write(
                    cnf,
                    Collections.singleton(mysqldConf + timezoneConf),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return Paths.get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }
}
