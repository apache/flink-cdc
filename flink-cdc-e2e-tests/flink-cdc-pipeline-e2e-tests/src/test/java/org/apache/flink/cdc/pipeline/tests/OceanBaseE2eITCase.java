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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestUtils;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseContainer;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.text.StringEscapeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** OceanBase flink cdc pipeline connector sink integrate test. */
class OceanBaseE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseE2eITCase.class);

    private static final String MYSQL_TEST_TABLE_NAME = "products";

    @Container
    private static final OceanBaseContainer OB_SERVER =
            OceanBaseTestUtils.createOceanBaseContainerForJdbc()
                    .withNetwork(NETWORK)
                    .withNetworkAliases("oceanbase")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    public final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(
                    MYSQL, "mysql_2_oceanbase_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private final String uniqueDatabaseName = mysqlInventoryDatabase.getDatabaseName();

    @BeforeEach
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n\n"
                                + "sink:\n"
                                + "  type: oceanbase\n"
                                + "  url: %s\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  name: oceanbase IT\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        uniqueDatabaseName,
                        getJdbcUrlInContainer("test", "oceanbase"),
                        OB_SERVER.getUsername(),
                        OB_SERVER.getPassword());
        Path oceanbaseCdcJar = TestUtils.getResource("oceanbase-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, oceanbaseCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitingAndAssertTableCount(MYSQL_TEST_TABLE_NAME, false, 9);
        List<String> originList = queryTable(MYSQL_TEST_TABLE_NAME, false);
        assertThat(originList)
                .containsExactlyInAnyOrderElementsOf(
                        Stream.of(
                                        "101,scooter,Small 2-wheel scooter,3.14,red,{\"key1\": \"value1\"},{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                                        "102,car battery,12V car battery,8.1,white,{\"key2\": \"value2\"},{\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,red,{\"key3\": \"value3\"},{\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                                        "104,hammer,12oz carpenter's hammer,0.75,white,{\"key4\": \"value4\"},{\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                                        "105,hammer,14oz carpenter's hammer,0.875,red,{\"k1\": \"v1\", \"k2\": \"v2\"},{\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                                        "106,hammer,16oz carpenter's hammer,1.0,null,null,null",
                                        "107,rocks,box of assorted rocks,5.3,null,null,null",
                                        "108,jacket,water resistent black wind breaker,0.1,null,null,null",
                                        "109,spare tire,24 inch spare tire,22.2,null,null,null")
                                .map(StringEscapeUtils::unescapeJava)
                                .collect(Collectors.toList()));
        // validate table of customers
        List<String> customerList = queryTable("customers", false);
        assertThat(customerList)
                .containsExactlyInAnyOrder(
                        "101,user_1,Shanghai,123567891234,2023-12-12T11:00:11",
                        "102,user_2,Shanghai,123567891234,2023-12-12T11:00:11",
                        "103,user_3,Shanghai,123567891234,2023-12-12T11:00:11",
                        "104,user_4,Shanghai,123567891234,2023-12-12T11:00:11");

        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), uniqueDatabaseName);
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            // modify table schema
            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);"); // 110
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);"); // 111
            stat.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products WHERE id=111;");
            stat.execute("ALTER TABLE products RENAME COLUMN new_col TO rename_col;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        waitingAndAssertTableCount(MYSQL_TEST_TABLE_NAME, false, 10);
        List<String> updateList = queryTable(MYSQL_TEST_TABLE_NAME, false);
        assertThat(updateList)
                .containsExactlyInAnyOrderElementsOf(
                        Stream.of(
                                        "101,scooter,Small 2-wheel scooter,3.14,red,{\"key1\": \"value1\"},{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0},null",
                                        "102,car battery,12V car battery,8.1,white,{\"key2\": \"value2\"},{\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0},null",
                                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,red,{\"key3\": \"value3\"},{\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0},null",
                                        "104,hammer,12oz carpenter's hammer,0.75,white,{\"key4\": \"value4\"},{\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0},null",
                                        "105,hammer,14oz carpenter's hammer,0.875,red,{\"k1\": \"v1\", \"k2\": \"v2\"},{\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0},null",
                                        "106,hammer,18oz carpenter hammer,1.0,null,null,null,null",
                                        "107,rocks,box of assorted rocks,5.1,null,null,null,null",
                                        "108,jacket,water resistent black wind breaker,0.1,null,null,null,null",
                                        "109,spare tire,24 inch spare tire,22.2,null,null,null,null",
                                        "110,jacket,new water resistent white wind breaker,0.5,null,null,null,1")
                                .map(StringEscapeUtils::unescapeJava)
                                .collect(Collectors.toList()));

        dropDatabase(getConnection(false), uniqueDatabaseName);
    }

    @Test
    public void testSyncWholeDatabaseInBatchMode() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "  scan.startup.mode: snapshot\n"
                                + "sink:\n"
                                + "  type: oceanbase\n"
                                + "  url: %s\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  name: oceanbase IT\n"
                                + "  parallelism: 1\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        uniqueDatabaseName,
                        getJdbcUrlInContainer("test", "oceanbase"),
                        OB_SERVER.getUsername(),
                        OB_SERVER.getPassword());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path oceanbaseCdcJar = TestUtils.getResource("oceanbase-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, oceanbaseCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitingAndAssertTableCount(MYSQL_TEST_TABLE_NAME, false, 9);
        List<String> originList = queryTable(MYSQL_TEST_TABLE_NAME, false);
        assertThat(originList)
                .containsExactlyInAnyOrderElementsOf(
                        Stream.of(
                                        "101,scooter,Small 2-wheel scooter,3.14,red,{\"key1\": \"value1\"},{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                                        "102,car battery,12V car battery,8.1,white,{\"key2\": \"value2\"},{\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,red,{\"key3\": \"value3\"},{\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                                        "104,hammer,12oz carpenter's hammer,0.75,white,{\"key4\": \"value4\"},{\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                                        "105,hammer,14oz carpenter's hammer,0.875,red,{\"k1\": \"v1\", \"k2\": \"v2\"},{\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                                        "106,hammer,16oz carpenter's hammer,1.0,null,null,null",
                                        "107,rocks,box of assorted rocks,5.3,null,null,null",
                                        "108,jacket,water resistent black wind breaker,0.1,null,null,null",
                                        "109,spare tire,24 inch spare tire,22.2,null,null,null")
                                .map(StringEscapeUtils::unescapeJava)
                                .collect(Collectors.toList()));
        // validate table of customers
        List<String> customerList = queryTable("customers", false);
        assertThat(customerList)
                .containsExactlyInAnyOrder(
                        "101,user_1,Shanghai,123567891234,2023-12-12T11:00:11",
                        "102,user_2,Shanghai,123567891234,2023-12-12T11:00:11",
                        "103,user_3,Shanghai,123567891234,2023-12-12T11:00:11",
                        "104,user_4,Shanghai,123567891234,2023-12-12T11:00:11");
    }

    private void waitingAndAssertTableCount(String tableName, boolean isMySQL, int expectedCount)
            throws InterruptedException {
        // waiting for databases were created in oceanbase to avoid get connection fail.
        Thread.sleep(10_000);

        int tableRowsCount = 0;
        for (int i = 0; i < 10; ++i) {
            tableRowsCount = getTableRowsCount(() -> getConnection(isMySQL), tableName);
            if (tableRowsCount < expectedCount) {
                Thread.sleep(100);
            }
        }
        assertThat(tableRowsCount).isEqualTo(expectedCount);
    }

    private List<String> queryTable(String tableName, boolean isMySQL) throws SQLException {
        return queryTable(tableName, isMySQL, Collections.singletonList("*"));
    }

    private List<String> queryTable(String tableName, boolean isMySQL, List<String> fields)
            throws SQLException {
        List<String> result = new ArrayList<>();
        try (Connection connection = getConnection(isMySQL);
                Statement statement = connection.createStatement()) {
            ResultSet rs =
                    statement.executeQuery(
                            "SELECT " + String.join(", ", fields) + " FROM " + tableName);
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(rs.getObject(i + 1));
                }
                result.add(sb.toString());
            }
        }
        return result;
    }

    private Connection getConnection(boolean isMySQL) throws SQLException {
        if (isMySQL) {
            String mysqlJdbcUrl =
                    String.format(
                            "jdbc:mysql://%s:%s/%s",
                            MYSQL.getHost(), MYSQL.getDatabasePort(), uniqueDatabaseName);
            return DriverManager.getConnection(mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
        }
        return DriverManager.getConnection(
                OB_SERVER.getJdbcUrl(uniqueDatabaseName),
                OB_SERVER.getUsername(),
                OB_SERVER.getPassword());
    }

    private int getTableRowsCount(
            SupplierWithException<Connection, SQLException> connectionSupplier, String tableName) {
        return (int)
                query(
                        connectionSupplier,
                        "SELECT COUNT(1) FROM " + tableName,
                        rs -> rs.next() ? rs.getInt(1) : 0);
    }

    private Object query(
            SupplierWithException<Connection, SQLException> connectionSupplier,
            String sql,
            FunctionWithException<ResultSet, Object, SQLException> resultSetConsumer) {
        try (Connection connection = connectionSupplier.get();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            return resultSetConsumer.apply(rs);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute sql: " + sql, e);
        }
    }

    private void dropDatabase(Connection connection, String database) {
        try (Connection conn = connection;
                Statement statement = conn.createStatement()) {
            statement.execute(String.format("DROP DATABASE %s", database));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop database", e);
        }
    }

    public String getJdbcUrlInContainer(String databaseName, String networkAliases) {
        return "jdbc:mysql://" + networkAliases + ":" + 2881 + "/" + databaseName;
    }
}
