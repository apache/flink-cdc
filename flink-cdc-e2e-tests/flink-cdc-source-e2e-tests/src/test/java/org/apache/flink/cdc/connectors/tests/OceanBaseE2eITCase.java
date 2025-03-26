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

package org.apache.flink.cdc.connectors.tests;

import org.apache.flink.cdc.common.test.utils.JdbcProxy;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.oceanbase.testutils.LogProxyContainer;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseCdcMetadata;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseContainer;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseMySQLCdcMetadata;
import org.apache.flink.cdc.connectors.oceanbase.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestUtils.createLogProxyContainer;
import static org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestUtils.createOceanBaseContainerForCDC;

/** End-to-end tests for oceanbase-cdc connector uber jar. */
@Testcontainers
class OceanBaseE2eITCase extends FlinkContainerTestEnvironment {

    private static final String INTER_CONTAINER_OB_SERVER_ALIAS = "oceanbase";
    private static final String INTER_CONTAINER_LOG_PROXY_ALIAS = "oblogproxy";

    private static final Path obCdcJar = TestUtils.getResource("oceanbase-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    @Container
    public static final OceanBaseContainer OB_SERVER =
            createOceanBaseContainerForCDC()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_OB_SERVER_ALIAS);

    @Container
    public static final LogProxyContainer LOG_PROXY =
            createLogProxyContainer()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_LOG_PROXY_ALIAS);

    private static final OceanBaseCdcMetadata METADATA =
            new OceanBaseMySQLCdcMetadata(OB_SERVER, LOG_PROXY);

    protected final UniqueDatabase obInventoryDatabase =
            new UniqueDatabase(OB_SERVER, "oceanbase_inventory");

    @BeforeEach
    public void before() {
        super.before();

        obInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();

        obInventoryDatabase.dropDatabase();
    }

    @Test
    void testOceanBaseCDC() throws Exception {
        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.checkpointing.interval' = '3s';",
                        "SET 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';",
                        "CREATE TABLE products_source (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " enum_c STRING,",
                        " json_c STRING,",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'oceanbase-cdc',",
                        " 'scan.startup.mode' = 'initial',",
                        " 'username' = '" + METADATA.getUsername() + "',",
                        " 'password' = '" + METADATA.getPassword() + "',",
                        " 'tenant-name' = '" + METADATA.getTenantName() + "',",
                        " 'table-list' = '"
                                + obInventoryDatabase.qualifiedTableName("products_source")
                                + "',",
                        " 'hostname' = '" + INTER_CONTAINER_OB_SERVER_ALIAS + "',",
                        " 'port' = '2881',",
                        " 'jdbc.driver' = '" + METADATA.getDriverClass() + "',",
                        " 'logproxy.host' = '" + INTER_CONTAINER_LOG_PROXY_ALIAS + "',",
                        " 'logproxy.port' = '2983',",
                        " 'rootserver-list' = '" + METADATA.getRsList() + "',",
                        " 'working-mode' = 'memory',",
                        " 'jdbc.properties.useSSL' = 'false'",
                        ");",
                        "CREATE TABLE ob_products_sink (",
                        " `id` INT NOT NULL,",
                        " name STRING,",
                        " description STRING,",
                        " weight DECIMAL(10,3),",
                        " enum_c STRING,",
                        " json_c STRING,",
                        " primary key (`id`) not enforced",
                        ") WITH (",
                        " 'connector' = 'jdbc',",
                        String.format(
                                " 'url' = 'jdbc:mysql://%s:3306/%s',",
                                INTER_CONTAINER_MYSQL_ALIAS,
                                mysqlInventoryDatabase.getDatabaseName()),
                        " 'table-name' = 'ob_products_sink',",
                        " 'username' = '" + MYSQL_TEST_USER + "',",
                        " 'password' = '" + MYSQL_TEST_PASSWORD + "'",
                        ");",
                        "INSERT INTO ob_products_sink",
                        "SELECT * FROM products_source;");

        submitSQLJob(sqlLines, obCdcJar, jdbcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        try (Connection conn = obInventoryDatabase.getJdbcConnection();
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE products_source SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products_source SET weight='5.1' WHERE id=107;");
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null);");
            stat.execute(
                    "INSERT INTO products_source VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null);");
            stat.execute(
                    "UPDATE products_source SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products_source SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products_source WHERE id=111;");
        } catch (SQLException e) {
            throw new RuntimeException("Update table for CDC failed.", e);
        }

        String mysqlUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        JdbcProxy proxy =
                new JdbcProxy(mysqlUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD, MYSQL_DRIVER_CLASS);
        List<String> expectResult =
                Arrays.asList(
                        "101,scooter,Small 2-wheel scooter,3.14,red,{\"key1\": \"value1\"}",
                        "102,car battery,12V car battery,8.1,white,{\"key2\": \"value2\"}",
                        "103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8,red,{\"key3\": \"value3\"}",
                        "104,hammer,12oz carpenter's hammer,0.75,white,{\"key4\": \"value4\"}",
                        "105,hammer,14oz carpenter's hammer,0.875,red,{\"k1\": \"v1\", \"k2\": \"v2\"}",
                        "106,hammer,18oz carpenter hammer,1.0,null,null",
                        "107,rocks,box of assorted rocks,5.1,null,null",
                        "108,jacket,water resistent black wind breaker,0.1,null,null",
                        "109,spare tire,24 inch spare tire,22.2,null,null",
                        "110,jacket,new water resistent white wind breaker,0.5,null,null");
        proxy.checkResultWithTimeout(
                expectResult,
                "ob_products_sink",
                new String[] {"id", "name", "description", "weight", "enum_c", "json_c"},
                300000L);
    }
}
