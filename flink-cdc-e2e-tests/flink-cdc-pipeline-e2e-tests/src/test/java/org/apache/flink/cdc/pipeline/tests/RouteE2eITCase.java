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
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/** E2e tests for routing features. */
@RunWith(Parameterized.class)
public class RouteE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(RouteE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";
    protected static final long EVENT_DEFAULT_TIMEOUT = 60000L;

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(
                                    MySqlVersion.V8_0) // v8 support both ARM and AMD architectures
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase routeTestDatabase =
            new UniqueDatabase(MYSQL, "route_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Before
    public void before() throws Exception {
        super.before();
        routeTestDatabase.createAndInitialize();
    }

    @After
    public void after() {
        super.after();
        routeTestDatabase.dropDatabase();
    }

    private void generateIncrementalChanges() throws SQLException {
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        routeTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7');");
            stat.execute("UPDATE TABLEBETA SET VERSION='2014' WHERE id=2014;");
            stat.execute("INSERT INTO TABLEGAMMA VALUES (3019, 'Emerald');");
            stat.execute("DELETE FROM TABLEDELTA WHERE id=4024;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
    }

    /** Generate schema change events. This must not be called until incremental stage starts. */
    private void generateSchemaChanges() throws SQLException {
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        routeTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("ALTER TABLE TABLEALPHA ADD COLUMN `NAME` VARCHAR(17);");
            stat.execute("INSERT INTO TABLEALPHA VALUES (10001, '12', 'Derrida');");

            stat.execute("ALTER TABLE TABLEBETA RENAME COLUMN `VERSION` TO `VERSION_EX`;");
            stat.execute("INSERT INTO TABLEBETA VALUES (10002, '15');");

            stat.execute(
                    "ALTER TABLE TABLEGAMMA CHANGE COLUMN `VERSION` `VERSION_EX` VARCHAR(19);");
            stat.execute("INSERT INTO TABLEGAMMA VALUES (10003, 'Fluorite');");

            stat.execute("ALTER TABLE TABLEDELTA DROP COLUMN `VERSION`;");
            stat.execute("INSERT INTO TABLEDELTA VALUES (10004);");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }
    }

    @Test
    public void testDefaultRoute() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));
        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));
        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));
        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        validateResult(
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2013, 13], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[2014, 14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3015, Amber], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3016, Black], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3017, Cyan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3018, Denim], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4019, Yosemite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4020, El Capitan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4021, Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4022, High Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4023, Mojave], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4024, Catalina], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        // generate binlogs
        generateIncrementalChanges();

        validateResult(
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();

        validateResult(
                "AddColumnEvent{tableId=%s.TABLEALPHA, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=%s.TABLEBETA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[10002, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VARCHAR(19)}}",
                "RenameColumnEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}");
    }

    @Test
    public void testMergeTableRoute() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    sink-table: %s.ALL\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.ALL, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        validateResult(
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2013, 13], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2014, 14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3015, Amber], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3016, Black], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3017, Cyan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3018, Denim], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4019, Yosemite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4020, El Capitan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4021, Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4022, High Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4023, Mojave], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4024, Catalina], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        // generate binlogs
        generateIncrementalChanges();

        validateResult(
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();

        validateResult(
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10002, null, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.ALL, nameMapping={VERSION=STRING}}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10003, null, null, Fluorite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10004, null, null, null], op=INSERT, meta=()}");
    }

    @Test
    public void testPartialRoute() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.(TABLEALPHA|TABLEBETA)\n"
                                + "    sink-table: NEW_%s.ALPHABET\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.ALPHABET, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2013, 13], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2014, 14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3015, Amber], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3016, Black], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3017, Cyan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3018, Denim], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4019, Yosemite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4020, El Capitan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4021, Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4022, High Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4023, Mojave], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4024, Catalina], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        // generate binlogs
        generateIncrementalChanges();

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();

        validateResult(
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10002, null, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VARCHAR(19)}}",
                "RenameColumnEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    @Test
    public void testMultipleRoute() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.(TABLEALPHA|TABLEBETA)\n"
                                + "    sink-table: NEW_%s.ALPHABET\n"
                                + "  - source-table: %s.(TABLEBETA|TABLEGAMMA)\n"
                                + "    sink-table: NEW_%s.BETAGAMM\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.ALPHABET, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.BETAGAMM, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2013, 13], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[2014, 14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[2012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[2013, 13], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[2014, 14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[3015, Amber], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[3016, Black], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[3017, Cyan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[3018, Denim], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4019, Yosemite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4020, El Capitan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4021, Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4022, High Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4023, Mojave], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[4024, Catalina], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        // generate binlogs
        generateIncrementalChanges();

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();

        validateResult(
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "AddColumnEvent{tableId=NEW_%s.BETAGAMM, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10002, null, null, 15], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[10002, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=NEW_%s.BETAGAMM, nameMapping={VERSION=STRING}}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[10003, null, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    @Test
    public void testOneToManyRoute() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    sink-table: NEW_%s.TABLEA\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    sink-table: NEW_%s.TABLEB\n"
                                + "  - source-table: %s.TABLEALPHA\n"
                                + "    sink-table: NEW_%s.TABLEC\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.TABLEA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.TABLEB, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.TABLEC, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[1011, 11], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        routeTestDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("INSERT INTO TABLEALPHA VALUES (3007, '7');");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[3007, 7], op=INSERT, meta=()}");

        generateSchemaChanges();

        validateResult(
                "AddColumnEvent{tableId=NEW_%s.TABLEA, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "AddColumnEvent{tableId=NEW_%s.TABLEB, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "AddColumnEvent{tableId=NEW_%s.TABLEC, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=%s.TABLEBETA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[10002, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VARCHAR(19)}}",
                "RenameColumnEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    @Test
    public void testMergeTableRouteWithTransform() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    projection: \\*, 'extras' AS EXTRAS\n"
                                + "route:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    sink-table: %s.ALL\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s.ALL, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`EXTRAS` STRING}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        validateResult(
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1008, 8, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1009, 8.1, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1010, 10, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[1011, 11, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2011, 11, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2012, 12, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2013, 13, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[2014, 14, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3015, Amber, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3016, Black, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3017, Cyan, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3018, Denim, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4019, Yosemite, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4020, El Capitan, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4021, Sierra, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4022, High Sierra, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4023, Mojave, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[4024, Catalina, extras], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        // generate binlogs
        generateIncrementalChanges();

        validateResult(
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3007, 7, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[2014, 14, extras], after=[2014, 2014, extras], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3019, Emerald, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[4024, Catalina, extras], after=[], op=DELETE, meta=()}");

        LOG.info("Begin schema changing stage.");
        generateSchemaChanges();

        validateResult(
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10001, 12, Derrida, extras], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10002, null, extras, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.ALL, nameMapping={VERSION=STRING}}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10003, null, extras, null, Fluorite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10004, null, extras, null, null], op=INSERT, meta=()}");
    }

    @Test
    public void testReplacementSymbol() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    sink-table: NEW_%s.NEW_<>\n"
                                + "    replace-symbol: <>\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 1",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName());
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.NEW_TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));
        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.NEW_TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));
        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.NEW_TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));
        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=NEW_%s.NEW_TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                        routeTestDatabase.getDatabaseName()));

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[1008, 8], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[1009, 8.1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[1010, 10], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[1011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[], after=[2011, 11], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[], after=[2012, 12], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[], after=[2013, 13], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[], after=[2014, 14], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[3015, Amber], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[3016, Black], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[3017, Cyan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[3018, Denim], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[4019, Yosemite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[4020, El Capitan], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[4021, Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[4022, High Sierra], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[4023, Mojave], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[4024, Catalina], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");

        generateIncrementalChanges();

        validateResult(
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();
        validateResult(
                "AddColumnEvent{tableId=NEW_%s.NEW_TABLEALPHA, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=NEW_%s.NEW_TABLEBETA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[], after=[10002, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, nameMapping={VERSION=VARCHAR(19)}}",
                "RenameColumnEvent{tableId=NEW_%s.NEW_TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=NEW_%s.NEW_TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    private void validateResult(String... expectedEvents) throws Exception {
        for (String event : expectedEvents) {
            waitUntilSpecificEvent(String.format(event, routeTestDatabase.getDatabaseName()));
        }
    }

    private void waitUntilSpecificEvent(String event) throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + EVENT_DEFAULT_TIMEOUT;
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = taskManagerConsumer.toUtf8String();
            if (stdout.contains(event)) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get specific event: "
                            + event
                            + " from stdout: "
                            + taskManagerConsumer.toUtf8String());
        }
    }

    private void assertNotExists(String event) {
        Assert.assertFalse(taskManagerConsumer.toUtf8String().contains(event));
    }
}
