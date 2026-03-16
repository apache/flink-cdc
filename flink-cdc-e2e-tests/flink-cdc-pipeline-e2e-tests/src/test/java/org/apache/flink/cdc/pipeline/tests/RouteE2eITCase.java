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

import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

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
import java.util.function.Function;
import java.util.stream.IntStream;

/** E2e tests for routing features. */
class RouteE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(RouteE2eITCase.class);

    protected static final int TEST_TABLE_NUMBER = 100;

    protected final UniqueDatabase routeTestDatabase =
            new UniqueDatabase(MYSQL, "route_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private final Function<String, String> routeDbNameFormatter =
            (s) -> String.format(s, routeTestDatabase.getDatabaseName());

    protected final UniqueDatabase extremeRouteTestDatabase =
            new UniqueDatabase(MYSQL, "extreme_route_test", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeEach
    public void before() throws Exception {
        super.before();
        routeTestDatabase.createAndInitialize();
    }

    @AfterEach
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
    void testDefaultRoute() throws Exception {
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
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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

        generateIncrementalChanges();
        validateResult(
                routeDbNameFormatter,
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();
        validateResult(
                routeDbNameFormatter,
                "AddColumnEvent{tableId=%s.TABLEALPHA, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.TABLEALPHA, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=%s.TABLEBETA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[10002, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEGAMMA, typeMapping={VERSION=VARCHAR(19)}, oldTypeMapping={VERSION=VARCHAR(17)}}",
                "RenameColumnEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}");
    }

    @Test
    void testDefaultRouteInBatchMode() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=%s.TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}");

        validateResult(
                routeDbNameFormatter,
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
    }

    @Test
    void testMergeTableRoute() throws Exception {
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
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=%s.ALL, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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

        generateIncrementalChanges();
        validateResult(
                routeDbNameFormatter,
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();
        validateResult(
                routeDbNameFormatter,
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=AFTER, existedColumnName=VERSION}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=AFTER, existedColumnName=NAME}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10002, null, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.ALL, typeMapping={VERSION=STRING}, oldTypeMapping={VERSION=VARCHAR(17)}}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10003, null, null, Fluorite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10004, null, null, null], op=INSERT, meta=()}");
    }

    @Test
    void testMergeTableRouteInBatchMode() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    sink-table: %s.ALL\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=%s.ALL, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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
    }

    @Test
    void testPartialRoute() throws Exception {
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
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.ALPHABET, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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
        generateIncrementalChanges();
        validateResult(
                routeDbNameFormatter,
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();
        validateResult(
                routeDbNameFormatter,
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=AFTER, existedColumnName=VERSION}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=AFTER, existedColumnName=NAME}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10002, null, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEGAMMA, typeMapping={VERSION=VARCHAR(19)}, oldTypeMapping={VERSION=VARCHAR(17)}}",
                "RenameColumnEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    @Test
    void testPartialRouteInBatchMode() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.(TABLEALPHA|TABLEBETA)\n"
                                + "    sink-table: NEW_%s.ALPHABET\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.ALPHABET, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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
    }

    @Test
    void testMultipleRoute() throws Exception {
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
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.ALPHABET, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.BETAGAMM, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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

        generateIncrementalChanges();
        validateResult(
                routeDbNameFormatter,
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();
        validateResult(
                routeDbNameFormatter,
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=AFTER, existedColumnName=VERSION}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=NEW_%s.ALPHABET, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=AFTER, existedColumnName=NAME}]}",
                "AddColumnEvent{tableId=NEW_%s.BETAGAMM, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=AFTER, existedColumnName=VERSION}]}",
                "DataChangeEvent{tableId=NEW_%s.ALPHABET, before=[], after=[10002, null, null, 15], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[10002, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=NEW_%s.BETAGAMM, typeMapping={VERSION=STRING}, oldTypeMapping={VERSION=VARCHAR(17)}}",
                "DataChangeEvent{tableId=NEW_%s.BETAGAMM, before=[], after=[10003, null, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    @Test
    void testMultipleRouteInBatchMode() throws Exception {
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
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.ALPHABET, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.BETAGAMM, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=%s.TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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
    }

    @Test
    void testOneToManyRoute() throws Exception {
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
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.TABLEA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.TABLEB, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.TABLEC, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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
                routeDbNameFormatter,
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[3007, 7], op=INSERT, meta=()}");

        generateSchemaChanges();
        validateResult(
                routeDbNameFormatter,
                "AddColumnEvent{tableId=NEW_%s.TABLEA, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "AddColumnEvent{tableId=NEW_%s.TABLEB, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "AddColumnEvent{tableId=NEW_%s.TABLEC, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.TABLEA, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEB, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.TABLEC, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=%s.TABLEBETA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEBETA, before=[], after=[10002, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.TABLEGAMMA, typeMapping={VERSION=VARCHAR(19)}, oldTypeMapping={VERSION=VARCHAR(17)}}",
                "RenameColumnEvent{tableId=%s.TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=%s.TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=%s.TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    @Test
    void testOneToManyRouteInBatchMode() throws Exception {
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
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.TABLEA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.TABLEB, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.TABLEC, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}");

        validateResult(
                routeDbNameFormatter,
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
    }

    @Test
    void testMergeTableRouteWithTransform() throws Exception {
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
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=%s.ALL, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`EXTRAS` STRING}, primaryKeys=ID, options=()}",
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

        generateIncrementalChanges();
        validateResult(
                routeDbNameFormatter,
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3007, 7, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[2014, 14, extras], after=[2014, 2014, extras], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[3019, Emerald, extras], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[4024, Catalina, extras], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();
        validateResult(
                routeDbNameFormatter,
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=AFTER, existedColumnName=EXTRAS}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10001, 12, extras, Derrida], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=%s.ALL, addedColumns=[ColumnWithPosition{column=`VERSION_EX` VARCHAR(17), position=AFTER, existedColumnName=NAME}]}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10002, null, extras, null, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.ALL, typeMapping={VERSION=STRING}, oldTypeMapping={VERSION=VARCHAR(17)}}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10003, null, extras, null, Fluorite], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.ALL, before=[], after=[10004, null, extras, null, null], op=INSERT, meta=()}");
    }

    @Test
    void testMergeTableRouteWithTransformInBatchMode() throws Exception {
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
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=%s.ALL, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17),`EXTRAS` STRING}, primaryKeys=ID, options=()}",
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
    }

    @Test
    void testReplacementSymbol() throws Exception {
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
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
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

        generateIncrementalChanges();

        validateResult(
                routeDbNameFormatter,
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[3007, 7], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[2014, 14], after=[2014, 2014], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[3019, Emerald], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[4024, Catalina], after=[], op=DELETE, meta=()}");

        generateSchemaChanges();
        validateResult(
                routeDbNameFormatter,
                "AddColumnEvent{tableId=NEW_%s.NEW_TABLEALPHA, addedColumns=[ColumnWithPosition{column=`NAME` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEALPHA, before=[], after=[10001, 12, Derrida], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=NEW_%s.NEW_TABLEBETA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEBETA, before=[], after=[10002, 15], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, typeMapping={VERSION=VARCHAR(19)}, oldTypeMapping={VERSION=VARCHAR(17)}}",
                "RenameColumnEvent{tableId=NEW_%s.NEW_TABLEGAMMA, nameMapping={VERSION=VERSION_EX}}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEGAMMA, before=[], after=[10003, Fluorite], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=NEW_%s.NEW_TABLEDELTA, droppedColumnNames=[VERSION]}",
                "DataChangeEvent{tableId=NEW_%s.NEW_TABLEDELTA, before=[], after=[10004], op=INSERT, meta=()}");
    }

    @Test
    void testReplacementSymbolInBatchMode() throws Exception {
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
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "route:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    sink-table: NEW_%s.NEW_<>\n"
                                + "    replace-symbol: <>\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        routeTestDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                routeDbNameFormatter,
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEALPHA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEBETA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEGAMMA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                "CreateTableEvent{tableId=NEW_%s.NEW_TABLEDELTA, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}");

        validateResult(
                routeDbNameFormatter,
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
    }

    @ParameterizedTest(name = "batchMode: {0}")
    @ValueSource(booleans = {true, false})
    void testExtremeMergeTableRoute(boolean batchMode) throws Exception {
        final String databaseName = extremeRouteTestDatabase.getDatabaseName();
        try (Connection conn =
                        DriverManager.getConnection(
                                MYSQL.getJdbcUrl(), MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute(String.format("CREATE DATABASE %s;", databaseName));
            stat.execute(String.format("USE %s;", databaseName));
            for (int i = 1; i <= TEST_TABLE_NUMBER; i++) {
                stat.execute(String.format("DROP TABLE IF EXISTS TABLE%d;", i));
                stat.execute(
                        String.format(
                                "CREATE TABLE TABLE%d (ID INT NOT NULL PRIMARY KEY,VERSION VARCHAR(17));",
                                i));
                stat.execute(String.format("INSERT INTO TABLE%d VALUES (%d, 'No.%d');", i, i, i));
            }
        } catch (SQLException e) {
            LOG.error("Initialize table failed.", e);
            throw e;
        }

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
                                + "  scan.startup.mode: %s\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + (batchMode ? "  execution.runtime-mode: BATCH" : ""),
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        batchMode ? "snapshot" : "initial",
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        String prefix = parallelism > 1 ? "> " : "";
        validateResult(
                IntStream.rangeClosed(1, TEST_TABLE_NUMBER)
                        .mapToObj(
                                i ->
                                        String.format(
                                                prefix
                                                        + "CreateTableEvent{tableId=%s.TABLE%d, schema=columns={`ID` INT NOT NULL,`VERSION` VARCHAR(17)}, primaryKeys=ID, options=()}",
                                                databaseName,
                                                i))
                        .toArray(String[]::new));
        validateResult(
                IntStream.rangeClosed(1, TEST_TABLE_NUMBER)
                        .mapToObj(
                                i ->
                                        String.format(
                                                prefix
                                                        + "DataChangeEvent{tableId=%s.TABLE%d, before=[], after=[%d, No.%d], op=INSERT, meta=()}",
                                                databaseName,
                                                i,
                                                i,
                                                i))
                        .toArray(String[]::new));
        extremeRouteTestDatabase.dropDatabase();
    }
}
