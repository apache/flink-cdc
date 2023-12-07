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

package com.ververica.cdc.connectors.tests;

import com.github.dockerjava.api.DockerClient;
import com.ververica.cdc.connectors.tests.utils.FlinkContainerTestEnvironment;
import com.ververica.cdc.connectors.tests.utils.JdbcProxy;
import com.ververica.cdc.connectors.tests.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/** End-to-end tests for oceanbase-cdc connector uber jar. */
public class OceanBaseE2eITCase extends FlinkContainerTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseE2eITCase.class);

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final Path obCdcJar = TestUtils.getResource("oceanbase-cdc-connector.jar");
    private static final Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

    // ------------------------------------------------------------------------------------------
    // OceanBase container variables
    // ------------------------------------------------------------------------------------------
    private static final String OB_SERVER_IMAGE = "oceanbase/oceanbase-ce:4.2.0.0";
    private static final String OB_LOG_PROXY_IMAGE = "whhe/oblogproxy:1.1.3_4x";
    private static final String NETWORK_MODE = "host";
    private static final String INTER_CONTAINER_OB_HOST = "host.docker.internal";
    private static final String SYS_PASSWORD = "1234567";
    private static final String TEST_TENANT = "test";
    private static final String TEST_USER = "root@" + TEST_TENANT;
    private static final String TEST_PASSWORD = "7654321";

    @ClassRule
    public static final GenericContainer<?> OB_SERVER =
            new GenericContainer<>(OB_SERVER_IMAGE)
                    .withNetworkMode(NETWORK_MODE)
                    .withEnv("MODE", "slim")
                    .withEnv("OB_DATAFILE_SIZE", "1G")
                    .withEnv("OB_LOG_DISK_SIZE", "4G")
                    .withEnv("OB_ROOT_PASSWORD", SYS_PASSWORD)
                    .withEnv("OB_TENANT_NAME", TEST_TENANT)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("docker/oceanbase/setup.sql"),
                            "/root/boot/init.d/init.sql")
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(Duration.ofMinutes(3))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> LOG_PROXY =
            new GenericContainer<>(OB_LOG_PROXY_IMAGE)
                    .withNetworkMode(NETWORK_MODE)
                    .withEnv("OB_SYS_PASSWORD", SYS_PASSWORD)
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(Duration.ofMinutes(1))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Before
    public void before() {
        super.before();

        initializeTable("oceanbase_inventory");
    }

    private Connection getTestConnection(String databaseName) {
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            return DriverManager.getConnection(
                    String.format("jdbc:mysql://127.0.0.1:2881/%s?useSSL=false", databaseName),
                    TEST_USER,
                    TEST_PASSWORD);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get test jdbc connection", e);
        }
    }

    @AfterClass
    public static void afterClass() {
        Stream.of(OB_SERVER, LOG_PROXY).forEach(GenericContainer::stop);

        DockerClient client = DockerClientFactory.instance().client();
        client.listImagesCmd()
                .withImageNameFilter(OB_SERVER_IMAGE)
                .exec()
                .forEach(image -> client.removeImageCmd(image.getId()).exec());
        client.listImagesCmd()
                .withImageNameFilter(OB_LOG_PROXY_IMAGE)
                .exec()
                .forEach(image -> client.removeImageCmd(image.getId()).exec());
    }

    @Test
    public void testOceanBaseCDC() throws Exception {
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
                        " 'username' = '" + TEST_USER + "',",
                        " 'password' = '" + TEST_PASSWORD + "',",
                        " 'tenant-name' = '" + TEST_TENANT + "',",
                        " 'table-list' = 'inventory.products_source',",
                        " 'hostname' = '" + INTER_CONTAINER_OB_HOST + "',",
                        " 'port' = '2881',",
                        " 'jdbc.driver' = '" + MYSQL_DRIVER_CLASS + "',",
                        " 'logproxy.host' = '" + INTER_CONTAINER_OB_HOST + "',",
                        " 'logproxy.port' = '2983',",
                        " 'rootserver-list' = '127.0.0.1:2882:2881',",
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

        try (Connection conn = getTestConnection("inventory");
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
                60000L);
    }

    protected void initializeTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = OceanBaseE2eITCase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getTestConnection("");
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
