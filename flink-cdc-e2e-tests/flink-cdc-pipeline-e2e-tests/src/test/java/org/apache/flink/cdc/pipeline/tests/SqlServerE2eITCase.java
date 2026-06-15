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
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;

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

/** End-to-end tests for sqlserver cdc pipeline job. */
public class SqlServerE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerE2eITCase.class);

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static final String INTER_CONTAINER_SQL_SERVER_ALIAS = "mssqlserver";
    private static final String MSSQL_SERVER_IMAGE = "mcr.microsoft.com/mssql/server:2019-latest";

    @Container
    public static final MSSQLServerContainer<?> SQL_SERVER_CONTAINER =
            new MSSQLServerContainer<>(MSSQL_SERVER_IMAGE)
                    .withPassword("Password!")
                    .withEnv("MSSQL_AGENT_ENABLED", "true")
                    .withEnv("MSSQL_PID", "Standard")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_SQL_SERVER_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeEach
    public void before() throws Exception {
        super.before();
        LOG.info("Starting SQL Server containers...");
        Startables.deepStart(Stream.of(SQL_SERVER_CONTAINER)).join();
        LOG.info("SQL Server containers are started.");
        initializeSqlServerTable("sqlserver_inventory");
    }

    @AfterEach
    public void after() {
        super.after();
        if (SQL_SERVER_CONTAINER != null) {
            SQL_SERVER_CONTAINER.stop();
        }
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: sqlserver\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: inventory.dbo.products\n"
                                + "  scan.startup.mode: initial\n"
                                + "  server-time-zone: UTC\n"
                                + "  schema-change.enabled: true\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_SQL_SERVER_ALIAS,
                        MSSQLServerContainer.MS_SQL_SERVER_PORT,
                        SQL_SERVER_CONTAINER.getUsername(),
                        SQL_SERVER_CONTAINER.getPassword(),
                        parallelism);
        Path sqlServerCdcJar = TestUtils.getResource("sqlserver-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, sqlServerCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                "CreateTableEvent{tableId=inventory.dbo.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL,`description` VARCHAR(512),`weight` DOUBLE}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[102, car battery, 12V car battery, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[107, rocks, box of assorted rocks, 5.3], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        try (Connection conn = getSqlServerJdbcConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("USE inventory;");
            stat.execute(
                    "INSERT INTO dbo.products(id,name,description,weight) VALUES (110,'jacket','water resistent white wind breaker',0.2);");
            stat.execute(
                    "UPDATE dbo.products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE dbo.products SET weight=5.1 WHERE id=107;");
            stat.execute("DELETE FROM dbo.products WHERE id=110;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=inventory.dbo.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.2], op=INSERT, meta=()}");
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=inventory.dbo.products, before=[106, hammer, 16oz carpenter's hammer, 1.0], after=[106, hammer, 18oz carpenter hammer, 1.0], op=UPDATE, meta=()}");
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=inventory.dbo.products, before=[107, rocks, box of assorted rocks, 5.3], after=[107, rocks, box of assorted rocks, 5.1], op=UPDATE, meta=()}");
        waitUntilSpecificEvent(
                "DataChangeEvent{tableId=inventory.dbo.products, before=[110, jacket, water resistent white wind breaker, 0.2], after=[], op=DELETE, meta=()}");
    }

    private void initializeSqlServerTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = SqlServerE2eITCase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try (Connection connection = getSqlServerJdbcConnection();
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

    private Connection getSqlServerJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                SQL_SERVER_CONTAINER.getJdbcUrl(),
                SQL_SERVER_CONTAINER.getUsername(),
                SQL_SERVER_CONTAINER.getPassword());
    }
}
