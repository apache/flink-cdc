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

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** E2e tests for the {@link PreTransformOperator} and {@link PostTransformOperator}. */
class TransformE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(TransformE2eITCase.class);

    protected final UniqueDatabase transformTestDatabase =
            new UniqueDatabase(MYSQL, "transform_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private final Function<String, String> dbNameFormatter =
            (s) -> {
                String databaseName = transformTestDatabase.getDatabaseName();
                return String.format(s, databaseName, databaseName, databaseName);
            };

    @BeforeEach
    public void before() throws Exception {
        super.before();
        transformTestDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        transformTestDatabase.dropDatabase();
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testHeteroSchemaTransform(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    sink-table: %s.terminus\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, VERSION\n"
                                + "    filter: ID > 1008\n"
                                + "  - source-table: %s.TABLEBETA\n"
                                + "    projection: ID, VERSION\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.terminus, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[2012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[2014, 14], op=INSERT, meta=()}");

        // Skip incremental stage if we're in batch mode
        if (batchMode) {
            return;
        }

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[1009, 8.1], after=[1009, 100], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[2011, 11], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testMultipleTransformRule(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "transform:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    projection: ID, VERSION, 'Type-A' AS CATEGORY\n"
                                + "    filter: ID > 1008\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    projection: ID, VERSION, 'Type-B' AS CATEGORY\n"
                                + "    filter: ID <= 1008\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`CATEGORY` STRING}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`CATEGORY` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1008, 8, Type-B], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, Type-A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, Type-A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, Type-A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11, Type-A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12, Type-A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13, Type-A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Type-A], op=INSERT, meta=()}");

        if (batchMode) {
            return;
        }

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, Type-A], after=[1009, 100, Type-A], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, Type-A], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, 11, Type-A], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testAssortedSchemaTransform(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    sink-table: %s.terminus\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, CONCAT('v', VERSION) AS VERSION, LOWER(NAMEALPHA) AS NAME\n"
                                + "    filter: AGEALPHA < 19\n"
                                + "  - source-table: %s.TABLEBETA\n"
                                + "    projection: ID, CONCAT('v', VERSION) AS VERSION, LOWER(NAMEBETA) AS NAME\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.terminus, schema=columns={`ID` INT NOT NULL,`VERSION` STRING,`NAME` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[1008, v8, alice], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[1009, v8.1, bob], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[2011, v11, eva], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[2012, v12, fred], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[2013, v13, gus], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[2014, v14, henry], op=INSERT, meta=()}");

        if (batchMode) {
            return;
        }

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.terminus, before=[1009, v8.1, bob], after=[1009, v100, bob], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[], after=[3007, v7, iina], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.terminus, before=[2011, v11, eva], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testWildcardSchemaTransform(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: \\*, CONCAT('v', VERSION) AS VERSION, LOWER(NAMEALPHA) AS NAME\n"
                                + "    filter: AGEALPHA < 19\n"
                                + "  - source-table: %s.TABLEBETA\n"
                                + "    projection: \\*, CONCAT('v', VERSION) AS VERSION, LOWER(NAMEBETA) AS NAME\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` STRING,`PRICEALPHA` INT,`AGEALPHA` INT,`NAMEALPHA` VARCHAR(128),`NAME` STRING}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` STRING,`CODENAMESBETA` VARCHAR(17),`AGEBETA` INT,`NAMEBETA` VARCHAR(128),`NAME` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1008, v8, 199, 17, Alice, alice], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, v8.1, 0, 18, Bob, bob], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, v11, Big Sur, 21, Eva, eva], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, v12, Monterey, 22, Fred, fred], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, v13, Ventura, 23, Gus, gus], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, v14, Sonoma, 24, Henry, henry], op=INSERT, meta=()}");

        if (batchMode) {
            return;
        }

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, v8.1, 0, 18, Bob, bob], after=[1009, v100, 0, 18, Bob, bob], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, v7, 79, 16, IINA, iina], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, v11, Big Sur, 21, Eva, eva], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testWildcardWithMetadataColumnTransform(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "  metadata.list: op_ts\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: \\*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ AS identifier_name, __data_event_type__ AS type, op_ts AS opts\n"
                                + "  - source-table: %s.TABLEBETA\n"
                                + "    projection: \\*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ AS identifier_name, __data_event_type__ AS type, op_ts AS opts\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`PRICEALPHA` INT,`AGEALPHA` INT,`NAMEALPHA` VARCHAR(128),`identifier_name` STRING,`type` STRING NOT NULL,`opts` BIGINT NOT NULL}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`CODENAMESBETA` VARCHAR(17),`AGEBETA` INT,`NAMEBETA` VARCHAR(128),`identifier_name` STRING,`type` STRING NOT NULL,`opts` BIGINT NOT NULL}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1008, 8, 199, 17, Alice, null.%s.TABLEALPHA, +I, 0], op=INSERT, meta=({op_ts=0})}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 99, 19, Carol, null.%s.TABLEALPHA, +I, 0], op=INSERT, meta=({op_ts=0})}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 0, 18, Bob, null.%s.TABLEALPHA, +I, 0], op=INSERT, meta=({op_ts=0})}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 59, 20, Dave, null.%s.TABLEALPHA, +I, 0], op=INSERT, meta=({op_ts=0})}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12, Monterey, 22, Fred, null.%s.TABLEBETA, +I, 0], op=INSERT, meta=({op_ts=0})}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11, Big Sur, 21, Eva, null.%s.TABLEBETA, +I, 0], op=INSERT, meta=({op_ts=0})}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Sonoma, 24, Henry, null.%s.TABLEBETA, +I, 0], op=INSERT, meta=({op_ts=0})}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13, Ventura, 23, Gus, null.%s.TABLEBETA, +I, 0], op=INSERT, meta=({op_ts=0})}");

        if (batchMode) {
            return;
        }

        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        insertBinlogEvents(mysqlJdbcUrl);

        validateEventsWithPattern(
                "DataChangeEvent\\{tableId=%s.TABLEALPHA, before=\\[1009, 8.1, 0, 18, Bob, null.%s.TABLEALPHA, -U, \\d+\\], after=\\[1009, 100, 0, 18, Bob, null.%s.TABLEALPHA, \\+U, \\d+\\], op=UPDATE, meta=\\(\\{op_ts=\\d+\\}\\)\\}",
                "DataChangeEvent\\{tableId=%s.TABLEALPHA, before=\\[\\], after=\\[3007, 7, 79, 16, IINA, null.%s.TABLEALPHA, \\+I, \\d+\\], op=INSERT, meta=\\(\\{op_ts=\\d+\\}\\)\\}",
                "DataChangeEvent\\{tableId=%s.TABLEBETA, before=\\[2011, 11, Big Sur, 21, Eva, null.%s.TABLEBETA, -D, \\d+\\], after=\\[\\], op=DELETE, meta=\\(\\{op_ts=\\d+\\}\\)\\}");
    }

    private static void insertBinlogEvents(String mysqlJdbcUrl) throws SQLException {
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testMultipleHittingTable(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLE\\.*\n"
                                + "    projection: \\*, ID + 1000 as UID, VERSION AS NEWVERSION\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`PRICEALPHA` INT,`AGEALPHA` INT,`NAMEALPHA` VARCHAR(128),`UID` INT,`NEWVERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`CODENAMESBETA` VARCHAR(17),`AGEBETA` INT,`NAMEBETA` VARCHAR(128),`UID` INT,`NEWVERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1008, 8, 199, 17, Alice, 2008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 0, 18, Bob, 2009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 99, 19, Carol, 2010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 59, 20, Dave, 2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11, Big Sur, 21, Eva, 3011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12, Monterey, 22, Fred, 3012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13, Ventura, 23, Gus, 3013, 13], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14, Sonoma, 24, Henry, 3014, 14], op=INSERT, meta=()}");

        if (batchMode) {
            return;
        }

        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 25, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 0, 18, Bob, 2009, 8.1], after=[1009, 100, 0, 18, Bob, 2009, 100], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 79, 25, IINA, 4007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, 11, Big Sur, 21, Eva, 3011, 11], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testMultipleTransformWithDiffRefColumn(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.TABLEALPHA\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, VERSION, PRICEALPHA, AGEALPHA, 'Juvenile' AS ROLENAME\n"
                                + "    filter: AGEALPHA < 18\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, VERSION, PRICEALPHA, AGEALPHA, NAMEALPHA AS ROLENAME\n"
                                + "    filter: AGEALPHA >= 18\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`PRICEALPHA` INT,`AGEALPHA` INT,`ROLENAME` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1008, 8, 199, 17, Juvenile], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 0, 18, Bob], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 99, 19, Carol], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 59, 20, Dave], op=INSERT, meta=()}");

        if (batchMode) {
            return;
        }

        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 25, 'IINA');");
            stat.execute("DELETE FROM TABLEALPHA WHERE id=1011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 0, 18, Bob], after=[1009, 100, 0, 18, Bob], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 79, 25, IINA], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1011, 11, 59, 20, Dave], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testTransformWithCast(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, CAST(VERSION AS DOUBLE) + 100 AS VERSION, CAST(AGEALPHA AS VARCHAR) || ' - ' || NAMEALPHA AS IDENTIFIER\n"
                                + "    filter: AGEALPHA < 19\n"
                                + "  - source-table: %s.TABLEBETA\n"
                                + "    projection: ID, CAST(VERSION AS DOUBLE) + 100 AS VERSION, CAST(AGEBETA AS VARCHAR) || ' - ' || NAMEBETA AS IDENTIFIER\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        if (batchMode) {
            return;
        }

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` DOUBLE,`IDENTIFIER` STRING}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` DOUBLE,`IDENTIFIER` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1008, 108.0, 17 - Alice], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 108.1, 18 - Bob], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 111.0, 21 - Eva], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 112.0, 22 - Fred], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 113.0, 23 - Gus], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 114.0, 24 - Henry], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 108.1, 18 - Bob], after=[1009, 200.0, 18 - Bob], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 107.0, 16 - IINA], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2011, 111.0, 21 - Eva], after=[], op=DELETE, meta=()}");
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testTemporalFunctions(boolean batchMode) throws Exception {
        String startupMode = batchMode ? "snapshot" : "initial";
        String runtimeMode = batchMode ? "BATCH" : "STREAMING";
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  scan.startup.mode: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    projection: ID, LOCALTIME as lcl_t, CURRENT_TIME as cur_t, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as cur_ts, CAST(NOW() AS TIMESTAMP) as now_ts, LOCALTIMESTAMP as lcl_ts, CURRENT_DATE as cur_dt\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  execution.runtime-mode: %s\n"
                                + "  parallelism: %d\n"
                                + "  local-time-zone: America/Los_Angeles",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        startupMode,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        runtimeMode,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitForTemporaryRecords(8, 60000L);
    }

    @Test
    void testTransformWithSchemaEvolution() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.TABLEALPHA\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID, 'id -> ' || ID AS UID, PRICEALPHA AS PRICE\n"
                                + "    filter: ID > 1008\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`UID` STRING,`PRICE` INT}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, id -> 1009, 0], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, id -> 1010, 99], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, id -> 1011, 59], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, id -> 1009, 0], after=[1009, id -> 1009, 0], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, id -> 3007, 79], op=INSERT, meta=()}");

        LOG.info("Start schema evolution.");
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {

            // triggers AddColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN CODENAME TINYINT AFTER VERSION;");
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN LAST VARCHAR(17);");
            stmt.execute("INSERT INTO TABLEALPHA VALUES (3008, '8', 8, 80, 17, 'Jazz', 'Last');");

            // triggers AlterColumnTypeEvent and RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA CHANGE COLUMN CODENAME CODE_NAME DOUBLE;");

            // triggers RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA RENAME COLUMN CODE_NAME TO CODE_NAME_EX;");
            stmt.execute("INSERT INTO TABLEALPHA VALUES (3009, '9', 9, 90, 18, 'Keka', 'Finale');");

            // triggers DropColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA DROP COLUMN CODE_NAME_EX");
            stmt.execute("INSERT INTO TABLEALPHA VALUES (3010, '10', 10, 97, 19, 'Lynx');");
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3008, id -> 3008, 80], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3009, id -> 3009, 90], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3010, id -> 3010, 10], op=INSERT, meta=()}");
    }

    @Test
    void testTransformWildcardPrefixWithSchemaEvolution() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.TABLEALPHA\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: \\*, 'id -> ' || ID AS UID\n"
                                + "    filter: ID > 1008\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`PRICEALPHA` INT,`AGEALPHA` INT,`NAMEALPHA` VARCHAR(128),`UID` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 0, 18, Bob, id -> 1009], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 99, 19, Carol, id -> 1010], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 59, 20, Dave, id -> 1011], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 0, 18, Bob, id -> 1009], after=[1009, 100, 0, 18, Bob, id -> 1009], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 79, 16, IINA, id -> 3007], op=INSERT, meta=()}");

        LOG.info("Start schema evolution.");
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {

            // triggers AddColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN CODENAME TINYINT AFTER VERSION;");
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN LAST VARCHAR(17);");
            stmt.execute("INSERT INTO TABLEALPHA VALUES (3008, '8', 8, 80, 17, 'Jazz', 'Last');");

            // triggers AlterColumnTypeEvent and RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA CHANGE COLUMN CODENAME CODE_NAME DOUBLE;");

            // triggers RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA RENAME COLUMN CODE_NAME TO CODE_NAME_EX;");
            stmt.execute("INSERT INTO TABLEALPHA VALUES (3009, '9', 9, 90, 18, 'Keka', 'Finale');");

            // triggers DropColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA DROP COLUMN CODE_NAME_EX");
            stmt.execute("INSERT INTO TABLEALPHA VALUES (3010, '10', 10, 97, 19, 'Lynx');");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "AddColumnEvent{tableId=%s.TABLEALPHA, addedColumns=[ColumnWithPosition{column=`LAST` VARCHAR(17), position=AFTER, existedColumnName=NAMEALPHA}]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3008, 8, 8, 80, 17, Jazz, Last, id -> 3008], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEALPHA, typeMapping={CODENAME=DOUBLE}, oldTypeMapping={CODENAME=TINYINT}}",
                "RenameColumnEvent{tableId=%s.TABLEALPHA, nameMapping={CODENAME=CODE_NAME}}",
                "RenameColumnEvent{tableId=%s.TABLEALPHA, nameMapping={CODE_NAME=CODE_NAME_EX}}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3009, 9, 9.0, 90, 18, Keka, Finale, id -> 3009], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEALPHA, droppedColumnNames=[CODE_NAME_EX]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3010, 10, 10, 97, 19, Lynx, id -> 3010], op=INSERT, meta=()}");
    }

    @Test
    void testTransformWildcardSuffixWithSchemaEvolution() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.TABLEALPHA\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    projection: ID || ' <- id' AS UID, *\n"
                                + "    filter: ID > 1008\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`UID` STRING,`ID` INT NOT NULL,`VERSION` VARCHAR(17),`PRICEALPHA` INT,`AGEALPHA` INT,`NAMEALPHA` VARCHAR(128)}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009 <- id, 1009, 8.1, 0, 18, Bob], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010 <- id, 1010, 10, 99, 19, Carol], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011 <- id, 1011, 11, 59, 20, Dave], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009 <- id, 1009, 8.1, 0, 18, Bob], after=[1009 <- id, 1009, 100, 0, 18, Bob], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007 <- id, 3007, 7, 79, 16, IINA], op=INSERT, meta=()}");

        LOG.info("Start schema evolution.");
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {

            // triggers AddColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN CODENAME TINYINT AFTER VERSION;");
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN FIRST VARCHAR(17) FIRST;");
            stmt.execute("INSERT INTO TABLEALPHA VALUES ('First', 3008, '8', 8, 80, 17, 'Jazz');");

            // triggers AlterColumnTypeEvent and RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA CHANGE COLUMN CODENAME CODE_NAME DOUBLE;");

            // triggers RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA RENAME COLUMN CODE_NAME TO CODE_NAME_EX;");
            stmt.execute("INSERT INTO TABLEALPHA VALUES ('1st', 3009, '9', 9, 90, 18, 'Keka');");

            // triggers DropColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA DROP COLUMN CODE_NAME_EX");
            stmt.execute(
                    "INSERT INTO TABLEALPHA VALUES ('Beginning', 3010, '10', 10, 97, 'Lemon');");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "AddColumnEvent{tableId=%s.TABLEALPHA, addedColumns=[ColumnWithPosition{column=`CODENAME` TINYINT, position=AFTER, existedColumnName=VERSION}]}",
                "AddColumnEvent{tableId=%s.TABLEALPHA, addedColumns=[ColumnWithPosition{column=`FIRST` VARCHAR(17), position=BEFORE, existedColumnName=ID}]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3008 <- id, First, 3008, 8, 8, 80, 17, Jazz], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEALPHA, typeMapping={CODENAME=DOUBLE}, oldTypeMapping={CODENAME=TINYINT}}",
                "RenameColumnEvent{tableId=%s.TABLEALPHA, nameMapping={CODENAME=CODE_NAME}}",
                "RenameColumnEvent{tableId=%s.TABLEALPHA, nameMapping={CODE_NAME=CODE_NAME_EX}}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3009 <- id, 1st, 3009, 9, 9.0, 90, 18, Keka], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEALPHA, droppedColumnNames=[CODE_NAME_EX]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3010 <- id, Beginning, 3010, 10, 10, 97, Lemon], op=INSERT, meta=()}");
    }

    private static final String[] UNICODE_STRINGS = {
        "ascii test!?",
        "大五",
        "测试数据",
        "ひびぴ",
        "죠주쥬",
        "ÀÆÉ",
        "ÓÔŐÖ",
        "αβγδε",
        "בבקשה",
        "твой",
        "ภาษาไทย",
        "piedzimst brīvi"
    };

    @Test
    void testTransformWithUnicodeLiterals() throws Exception {
        StringBuilder projectionExpression = new StringBuilder("\\*,");
        for (int i = 0; i < UNICODE_STRINGS.length; i++) {
            projectionExpression
                    .append('\'')
                    .append(UNICODE_STRINGS[i])
                    .append('\'')
                    .append(" AS col_")
                    .append(i)
                    .append(",");
        }
        projectionExpression.deleteCharAt(projectionExpression.length() - 1);

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.TABLEALPHA\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    projection: %s\n"
                                + "    filter: ID > 1008\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        transformTestDatabase.getDatabaseName(),
                        transformTestDatabase.getDatabaseName(),
                        projectionExpression,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`PRICEALPHA` INT,`AGEALPHA` INT,`NAMEALPHA` VARCHAR(128),`col_0` STRING,`col_1` STRING,`col_2` STRING,`col_3` STRING,`col_4` STRING,`col_5` STRING,`col_6` STRING,`col_7` STRING,`col_8` STRING,`col_9` STRING,`col_10` STRING,`col_11` STRING}, primaryKeys=ID, options=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10, 99, 19, Carol, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1, 0, 18, Bob, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11, 59, 20, Dave, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        transformTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE TABLEALPHA SET VERSION='100' WHERE id=1009;");
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7', 79, 16, 'IINA');");
            stat.execute("DELETE FROM TABLEBETA WHERE id=2011;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[1009, 8.1, 0, 18, Bob, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], after=[1009, 100, 0, 18, Bob, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7, 79, 16, IINA, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=INSERT, meta=()}");

        LOG.info("Start schema evolution.");
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {

            // triggers AddColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN CODENAME TINYINT AFTER VERSION;");
            stmt.execute("ALTER TABLE TABLEALPHA ADD COLUMN FIRST VARCHAR(17) FIRST;");
            stmt.execute("INSERT INTO TABLEALPHA VALUES ('First', 3008, '8', 8, 80, 17, 'Jazz');");

            // triggers AlterColumnTypeEvent and RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA CHANGE COLUMN CODENAME CODE_NAME DOUBLE;");

            // triggers RenameColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA RENAME COLUMN CODE_NAME TO CODE_NAME_EX;");
            stmt.execute("INSERT INTO TABLEALPHA VALUES ('1st', 3009, '9', 9, 90, 18, 'Keka');");

            // triggers DropColumnEvent
            stmt.execute("ALTER TABLE TABLEALPHA DROP COLUMN CODE_NAME_EX");
            stmt.execute(
                    "INSERT INTO TABLEALPHA VALUES ('Beginning', 3010, '10', 10, 97, 'Lemon');");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "AddColumnEvent{tableId=%s.TABLEALPHA, addedColumns=[ColumnWithPosition{column=`CODENAME` TINYINT, position=AFTER, existedColumnName=VERSION}]}",
                "AddColumnEvent{tableId=%s.TABLEALPHA, addedColumns=[ColumnWithPosition{column=`FIRST` VARCHAR(17), position=BEFORE, existedColumnName=ID}]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[First, 3008, 8, 8, 80, 17, Jazz, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEALPHA, typeMapping={CODENAME=DOUBLE}, oldTypeMapping={CODENAME=TINYINT}}",
                "RenameColumnEvent{tableId=%s.TABLEALPHA, nameMapping={CODENAME=CODE_NAME}}",
                "RenameColumnEvent{tableId=%s.TABLEALPHA, nameMapping={CODE_NAME=CODE_NAME_EX}}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1st, 3009, 9, 9.0, 90, 18, Keka, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEALPHA, droppedColumnNames=[CODE_NAME_EX]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[Beginning, 3010, 10, 10, 97, Lemon, ascii test!?, 大五, 测试数据, ひびぴ, 죠주쥬, ÀÆÉ, ÓÔŐÖ, αβγδε, בבקשה, твой, ภาษาไทย, piedzimst brīvi], op=INSERT, meta=()}");
    }

    private void validateEventsWithPattern(String... patterns) throws Exception {
        for (String pattern : patterns) {
            waitUntilSpecificEventWithPattern(
                    String.format(
                            pattern,
                            transformTestDatabase.getDatabaseName(),
                            transformTestDatabase.getDatabaseName(),
                            transformTestDatabase.getDatabaseName()),
                    20000L);
        }
    }

    private void waitUntilSpecificEventWithPattern(String patternStr, long timeout)
            throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = taskManagerConsumer.toUtf8String();
            Pattern pattern = Pattern.compile(patternStr);
            if (pattern.matcher(stdout).find()) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get events with pattern: "
                            + patternStr
                            + " from stdout: "
                            + taskManagerConsumer.toUtf8String());
        }
    }

    private int validateTemporaryRecords() {
        int validRecordCount = 0;
        for (String line : taskManagerConsumer.toUtf8String().split("\n")) {
            if (extractDataLines(line)) {
                validRecordCount++;
            }
        }
        return validRecordCount;
    }

    private void waitForTemporaryRecords(int expectedRecords, long timeout) throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTimeout) {
            if (validateTemporaryRecords() >= expectedRecords) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get enough temporary records: "
                            + expectedRecords
                            + " from stdout: "
                            + taskManagerConsumer.toUtf8String());
        }
    }

    boolean extractDataLines(String line) {
        // In multiple parallelism mode, a prefix with subTaskId (like '1> ') will be appended.
        // Should trim it before extracting data fields.
        if (parallelism > 1) {
            if (!line.startsWith("DataChangeEvent{", 3)) {
                return false;
            }
        } else {
            if (!line.startsWith("DataChangeEvent{")) {
                return false;
            }
        }
        Stream.of("before", "after")
                .forEach(
                        tag -> {
                            String[] arr = line.split(tag + "=\\[", 2);
                            String dataRecord = arr[arr.length - 1].split("]", 2)[0];
                            if (!dataRecord.isEmpty()) {
                                verifyDataRecord(dataRecord);
                            }
                        });
        return true;
    }

    void verifyDataRecord(String recordLine) {
        List<String> tokens = Arrays.asList(recordLine.split(", "));
        assertThat(tokens).hasSizeGreaterThanOrEqualTo(6);

        tokens = tokens.subList(tokens.size() - 6, tokens.size());

        String localTime = tokens.get(0);
        String currentTime = tokens.get(1);
        assertThat(localTime).isEqualTo(currentTime);

        String currentTimestamp = tokens.get(2);
        String nowTimestamp = tokens.get(3);
        String localTimestamp = tokens.get(4);
        assertThat(currentTimestamp).isEqualTo(nowTimestamp).isEqualTo(localTimestamp);

        // If timestamp millisecond part is .000, it will be truncated to yyyy-MM-dd'T'HH:mm:ss
        // format. Manually append this for the following checks.
        if (currentTimestamp.length() == 19) {
            currentTimestamp += ".000";
        }

        Instant instant =
                LocalDateTime.parse(
                                currentTimestamp,
                                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"))
                        .toInstant(ZoneOffset.UTC);

        long milliSecondsInOneDay = 24 * 60 * 60 * 1000;
        assertThat(TimeData.fromIsoLocalTimeString(localTime))
                .isEqualTo(
                        TimeData.fromMillisOfDay(
                                (int) (instant.toEpochMilli() % milliSecondsInOneDay)));

        String localDate = tokens.get(5);
        assertThat(DateData.fromIsoLocalDateString(localDate))
                .isEqualTo(
                        DateData.fromEpochDay(
                                (int) (instant.toEpochMilli() / milliSecondsInOneDay)));
    }
}
