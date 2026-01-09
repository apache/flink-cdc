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
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.TOP_SECRET;
import static org.apache.flink.cdc.connectors.oracle.source.OracleSourceTestBase.TOP_USER;

/** End-to-end tests for Oracle cdc pipeline job. */
public class OracleE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(OracleE2eITCase.class);
    protected static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private static OracleContainer oracle;
    // ------------------------------------------------------------------------------------------
    // Oracle Variables (we always use Oracle as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    public static final String ORACLE_DATABASE = "ORCLCDB";
    public static final String CONNECTOR_USER = "dbzuser";
    public static final String CONNECTOR_PWD = "dbz";
    public static final String TEST_USER = "debezium";
    public static final String TEST_PWD = "dbz";
    public static final String ORACLE_IMAGE = "goodboy008/oracle-19.3.0-ee";
    private static final String INTER_CONTAINER_ORACLE_ALIAS = "oracle";
    private static final Path oracleOjdbcJar = TestUtils.getResource("oracle-ojdbc.jar");

    @BeforeEach
    public void before() throws Exception {
        super.before();
        LOG.info("Starting containers...");

        oracle =
                new OracleContainer(
                                DockerImageName.parse(ORACLE_IMAGE)
                                        .withTag(
                                                DockerClientFactory.instance()
                                                                .client()
                                                                .versionCmd()
                                                                .exec()
                                                                .getArch()
                                                                .equals("amd64")
                                                        ? "non-cdb"
                                                        : "arm-non-cdb"))
                        .withUsername(CONNECTOR_USER)
                        .withPassword(CONNECTOR_PWD)
                        .withDatabaseName(ORACLE_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_ORACLE_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG))
                        .withReuse(true);

        Startables.deepStart(Stream.of(oracle)).join();
        initializeOracleTable("product");
        LOG.info("Containers are started.");
    }

    @AfterEach
    public void after() {
        super.after();
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: oracle\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.PRODUCTS\n"
                                + "  database: %s\n"
                                + "  scan.startup.mode: initial\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        oracle.getNetworkAliases().get(0),
                        oracle.getExposedPorts().get(0),
                        TOP_USER,
                        TOP_SECRET,
                        "DEBEZIUM",
                        ORACLE_DATABASE,
                        1);
        Path postgresCdcJar = TestUtils.getResource("oracle-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, postgresCdcJar, oracleOjdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        try (Connection conn = getOracleJdbcConnection();
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "UPDATE DEBEZIUM.PRODUCTS SET DESCRIPTION='18oz carpenter hammer' WHERE ID=106;");
            stat.execute("UPDATE DEBEZIUM.PRODUCTS SET WEIGHT='5.1' WHERE ID=107;");

            // Perform DML changes after the redo log is generated
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=DEBEZIUM.PRODUCTS, before=[106, hammer, 16oz carpenter's hammer, 1.0], after=[106, hammer, 18oz carpenter hammer, 1.0], op=UPDATE, meta=()}");
            waitUntilSpecificEvent(
                    "DataChangeEvent{tableId=DEBEZIUM.PRODUCTS, before=[107, rocks, box of assorted rocks, 5.3], after=[107, rocks, box of assorted rocks, 5.1], op=UPDATE, meta=()}");
        } catch (Exception e) {
            LOG.error("Update table for CDC failed.", e);
            throw new RuntimeException(e);
        }
    }

    private static void initializeOracleTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = OracleSourceTestBase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertThat(ddlTestFile).withFailMessage("Cannot locate " + ddlFile).isNotNull();
        try (Connection connection = getOracleJdbcConnection();
                Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            // region Drop all user tables in Debezium schema
            listTables(connection)
                    .forEach(
                            tableId -> {
                                try {
                                    statement.execute(
                                            "DROP TABLE "
                                                    + String.join(
                                                            ".",
                                                            tableId.schema(),
                                                            tableId.table()));
                                } catch (SQLException e) {
                                    LOG.warn("drop table error, table:{}", tableId, e);
                                }
                            });
            // endregion

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
        } catch (SQLException | IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection getOracleJdbcConnection() throws SQLException {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new FlinkRuntimeException("not found driver: " + e.getMessage());
        }
        return DriverManager.getConnection(oracle.getJdbcUrl(), TEST_USER, TEST_PWD);
    }

    protected static List<TableId> listTables(Connection connection) {

        Set<TableId> tableIdSet = new HashSet<>();
        String queryTablesSql =
                "SELECT OWNER ,TABLE_NAME,TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME NOT IN ('SYSTEM','SYSAUX') "
                        + "AND NESTED = 'NO' AND TABLE_NAME NOT IN (SELECT PARENT_TABLE_NAME FROM ALL_NESTED_TABLES)";
        try {
            ResultSet resultSet = connection.createStatement().executeQuery(queryTablesSql);
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1);
                String tableName = resultSet.getString(2);
                TableId tableId = new TableId(ORACLE_DATABASE, schemaName, tableName);
                tableIdSet.add(tableId);
            }
        } catch (SQLException e) {
            LOG.warn(" SQL execute error, sql:{}", queryTablesSql, e);
        }
        return new ArrayList<>(tableIdSet);
    }
}
