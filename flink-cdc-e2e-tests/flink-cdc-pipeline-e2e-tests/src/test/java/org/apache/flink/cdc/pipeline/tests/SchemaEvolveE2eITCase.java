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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.ToStringConsumer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** E2e tests for Schema Evolution cases. */
public class SchemaEvolveE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolveE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";
    protected static final long EVENT_WAITING_TIMEOUT = 60000L;

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

    protected final UniqueDatabase schemaEvolveDatabase =
            new UniqueDatabase(MYSQL, "schema_evolve", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @Before
    public void before() throws Exception {
        super.before();
        schemaEvolveDatabase.createAndInitialize();
    }

    @After
    public void after() {
        super.after();
        schemaEvolveDatabase.dropDatabase();
    }

    @Test
    public void testSchemaEvolve() throws Exception {
        testGenericSchemaEvolution(
                "evolve",
                false,
                false,
                false,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012, Eve, 17, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={age=precise_age}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}",
                        "DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013, Fiona, 16.0], op=INSERT, meta=()}",
                        "TruncateTableEvent{tableId=%s.members}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014, Gem, 17.0], op=INSERT, meta=()}",
                        "DropTableEvent{tableId=%s.members}"));
    }

    @Test
    public void testSchemaEvolveWithIncompatibleChanges() throws Exception {
        testGenericSchemaEvolution(
                "evolve",
                true,
                false,
                false,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.merged, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=%s.merged, before=[], after=[1012, Eve, 17, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.merged, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "AddColumnEvent{tableId=%s.merged, addedColumns=[ColumnWithPosition{column=`precise_age` DOUBLE, position=AFTER, existedColumnName=gender}]}",
                        "AddColumnEvent{tableId=%s.merged, addedColumns=[ColumnWithPosition{column=`biological_sex` TINYINT, position=AFTER, existedColumnName=precise_age}]}",
                        "DataChangeEvent{tableId=%s.merged, before=[], after=[1013, Fiona, null, null, 16.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.merged, before=[], after=[1014, Gem, null, null, 17.0, null], op=INSERT, meta=()}"));
    }

    @Test
    public void testSchemaEvolveWithException() throws Exception {
        testGenericSchemaEvolution(
                "evolve",
                false,
                true,
                false,
                Collections.emptyList(),
                Arrays.asList(
                        "Caused by: org.apache.flink.util.FlinkRuntimeException: Failed to apply schema change event.",
                        "Caused by: org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException",
                        "org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy"));
    }

    @Test
    public void testSchemaTryEvolveWithException() throws Exception {
        testGenericSchemaEvolution(
                "try_evolve",
                false,
                true,
                false,
                Arrays.asList(
                        // Add column never succeeded, so age column will not appear.
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012, Eve, 17], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013, Fiona, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014, Gem, null], op=INSERT, meta=()}"),
                Arrays.asList(
                        "Failed to apply schema change AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change RenameColumnEvent{tableId=%s.members, nameMapping={age=precise_age}}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=RenameColumnEvent{tableId=%s.members, nameMapping={age=precise_age}}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change TruncateTableEvent{tableId=%s.members}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=TruncateTableEvent{tableId=%s.members}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change DropTableEvent{tableId=%s.members}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=DropTableEvent{tableId=%s.members}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}"));
    }

    @Test
    public void testSchemaIgnore() throws Exception {

        testGenericSchemaEvolution(
                "ignore",
                false,
                false,
                false,
                Arrays.asList(
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012, Eve, 17], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013, Fiona, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014, Gem, null], op=INSERT, meta=()}"));
    }

    @Test
    public void testSchemaException() throws Exception {
        testGenericSchemaEvolution(
                "exception",
                false,
                false,
                false,
                Collections.emptyList(),
                Arrays.asList(
                        "An exception was triggered from Schema change applying task. Job will fail now.",
                        "org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy"));
    }

    @Test
    public void testLenientSchemaEvolution() throws Exception {

        testGenericSchemaEvolution(
                "lenient",
                false,
                false,
                false,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012, Eve, 17, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`precise_age` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`biological_sex` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013, Fiona, null, null, 16.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014, Gem, null, null, 17.0, null], op=INSERT, meta=()}"));

        assertNotExists(
                Collections.singletonList(
                        "Applied schema change event DropTableEvent{tableId=%s.members}"),
                taskManagerConsumer);
    }

    @Test
    public void testFineGrainedSchemaEvolution() throws Exception {

        testGenericSchemaEvolution(
                "evolve",
                false,
                false,
                true,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012, Eve, 17, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={age=precise_age}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013, Fiona, 16.0, null], op=INSERT, meta=()}",
                        "TruncateTableEvent{tableId=%s.members}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014, Gem, 17.0, null], op=INSERT, meta=()}"),
                Arrays.asList(
                        "Ignored schema change DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}.",
                        "Ignored schema change DropTableEvent{tableId=%s.members}."));
    }

    @Test
    public void testLenientWithRoute() throws Exception {
        String dbName = schemaEvolveDatabase.getDatabaseName();

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.members\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "route:\n"
                                + "  - source-table: %s.members\n"
                                + "    sink-table: %s.redirect\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: lenient\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        dbName,
                        dbName,
                        dbName,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        validateSnapshotData(dbName, "redirect");

        LOG.info("Starting schema evolution");
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s", MYSQL.getHost(), MYSQL.getDatabasePort(), dbName);

        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {

            waitForIncrementalStage(dbName, "redirect", stmt);

            // triggers AddColumnEvent
            stmt.execute("ALTER TABLE members ADD COLUMN gender TINYINT AFTER age;");
            stmt.execute("INSERT INTO members VALUES (1012, 'Eve', 17, 0);");

            // triggers AlterColumnTypeEvent and RenameColumnEvent
            stmt.execute("ALTER TABLE members CHANGE COLUMN age precise_age DOUBLE;");

            // triggers RenameColumnEvent
            stmt.execute("ALTER TABLE members RENAME COLUMN gender TO biological_sex;");

            // triggers DropColumnEvent
            stmt.execute("ALTER TABLE members DROP COLUMN biological_sex");
            stmt.execute("INSERT INTO members VALUES (1013, 'Fiona', 16);");

            // triggers TruncateTableEvent
            stmt.execute("TRUNCATE TABLE members;");
            stmt.execute("INSERT INTO members VALUES (1014, 'Gem', 17);");

            // triggers DropTableEvent
            stmt.execute("DROP TABLE members;");
        }

        List<String> expectedTaskManagerEvents =
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.redirect, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=%s.redirect, before=[], after=[1012, Eve, 17, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.redirect, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "AddColumnEvent{tableId=%s.redirect, addedColumns=[ColumnWithPosition{column=`precise_age` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=%s.redirect, addedColumns=[ColumnWithPosition{column=`biological_sex` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=%s.redirect, before=[], after=[1013, Fiona, null, null, 16.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.redirect, before=[], after=[1014, Gem, null, null, 17.0, null], op=INSERT, meta=()}");

        List<String> expectedTmEvents =
                expectedTaskManagerEvents.stream()
                        .map(s -> String.format(s, dbName, dbName))
                        .collect(Collectors.toList());

        validateResult(expectedTmEvents, taskManagerConsumer);
    }

    @Test
    public void testUnexpectedBehavior() {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.members\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: unexpected\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        schemaEvolveDatabase.getDatabaseName(),
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");

        // Submitting job should fail given an unknown schema change behavior configuration
        Assert.assertThrows(
                AssertionError.class,
                () -> submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar));
    }

    @Test
    public void testByDefaultTransform() throws Exception {
        String dbName = schemaEvolveDatabase.getDatabaseName();

        // We put a dummy transform block that matches nothing
        // to ensure TransformOperator exists, so we could verify if TransformOperator could
        // correctly handle such "bypass" tables with schema changes.
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.members\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "transform:\n"
                                + "  - source-table: another.irrelevant\n"
                                + "    projection: \"'irrelevant' AS tag\"\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: evolve\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        dbName,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        validateSnapshotData(dbName, "members");

        LOG.info("Starting schema evolution");
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s", MYSQL.getHost(), MYSQL.getDatabasePort(), dbName);

        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {

            waitForIncrementalStage(dbName, "members", stmt);

            // triggers AddColumnEvent
            stmt.execute("ALTER TABLE members ADD COLUMN gender TINYINT AFTER age;");
            stmt.execute("INSERT INTO members VALUES (1012, 'Eve', 17, 0);");

            // triggers AlterColumnTypeEvent and RenameColumnEvent
            stmt.execute("ALTER TABLE members CHANGE COLUMN age precise_age DOUBLE;");

            // triggers RenameColumnEvent
            stmt.execute("ALTER TABLE members RENAME COLUMN gender TO biological_sex;");

            // triggers DropColumnEvent
            stmt.execute("ALTER TABLE members DROP COLUMN biological_sex");
            stmt.execute("INSERT INTO members VALUES (1013, 'Fiona', 16);");

            // triggers TruncateTableEvent
            stmt.execute("TRUNCATE TABLE members;");
            stmt.execute("INSERT INTO members VALUES (1014, 'Gem', 17);");

            // triggers DropTableEvent
            stmt.execute("DROP TABLE members;");
        }

        List<String> expectedTaskManagerEvents =
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012, Eve, 17, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={age=precise_age}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}",
                        "DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013, Fiona, 16.0], op=INSERT, meta=()}",
                        "TruncateTableEvent{tableId=%s.members}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014, Gem, 17.0], op=INSERT, meta=()}",
                        "DropTableEvent{tableId=%s.members}");

        List<String> expectedTmEvents =
                expectedTaskManagerEvents.stream()
                        .map(s -> String.format(s, dbName, dbName))
                        .collect(Collectors.toList());

        validateResult(expectedTmEvents, taskManagerConsumer);
    }

    private void testGenericSchemaEvolution(
            String behavior,
            boolean mergeTable,
            boolean triggerError,
            boolean fineGrained,
            List<String> expectedTaskManagerEvents)
            throws Exception {
        testGenericSchemaEvolution(
                behavior,
                mergeTable,
                triggerError,
                fineGrained,
                expectedTaskManagerEvents,
                Collections.emptyList());
    }

    private void testGenericSchemaEvolution(
            String behavior,
            boolean mergeTable,
            boolean triggerError,
            boolean fineGrained,
            List<String> expectedTaskManagerEvents,
            List<String> expectedJobManagerEvents)
            throws Exception {

        String dbName = schemaEvolveDatabase.getDatabaseName();

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.%s\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + (fineGrained
                                        ? "  exclude.schema.changes:\n" + "    - drop\n"
                                        : "")
                                + (triggerError ? "  error.on.schema.change: true" : "")
                                + (mergeTable
                                        ? String.format(
                                                "route:\n"
                                                        + "  - source-table: %s.(members|new_members)\n"
                                                        + "    sink-table: %s.merged",
                                                dbName, dbName)
                                        : "")
                                + "\n"
                                + "pipeline:\n"
                                + "  schema.change.behavior: %s\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        dbName,
                        mergeTable ? "(members|new_members)" : "members",
                        behavior,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
        validateSnapshotData(dbName, mergeTable ? "merged" : "members");

        LOG.info("Starting schema evolution");
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s", MYSQL.getHost(), MYSQL.getDatabasePort(), dbName);

        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stmt = conn.createStatement()) {

            waitForIncrementalStage(dbName, mergeTable ? "merged" : "members", stmt);

            // triggers AddColumnEvent
            stmt.execute("ALTER TABLE members ADD COLUMN gender TINYINT AFTER age;");
            stmt.execute("INSERT INTO members VALUES (1012, 'Eve', 17, 0);");

            // triggers AlterColumnTypeEvent and RenameColumnEvent
            stmt.execute("ALTER TABLE members CHANGE COLUMN age precise_age DOUBLE;");

            // triggers RenameColumnEvent
            stmt.execute("ALTER TABLE members RENAME COLUMN gender TO biological_sex;");

            // triggers DropColumnEvent
            stmt.execute("ALTER TABLE members DROP COLUMN biological_sex");
            stmt.execute("INSERT INTO members VALUES (1013, 'Fiona', 16);");

            // triggers TruncateTableEvent
            stmt.execute("TRUNCATE TABLE members;");
            stmt.execute("INSERT INTO members VALUES (1014, 'Gem', 17);");

            // triggers DropTableEvent
            stmt.execute("DROP TABLE members;");
        }

        List<String> expectedTmEvents =
                expectedTaskManagerEvents.stream()
                        .map(s -> String.format(s, dbName, dbName))
                        .collect(Collectors.toList());

        validateResult(expectedTmEvents, taskManagerConsumer);

        List<String> expectedJmEvents =
                expectedJobManagerEvents.stream()
                        .map(s -> String.format(s, dbName, dbName, dbName))
                        .collect(Collectors.toList());

        validateResult(expectedJmEvents, jobManagerConsumer);
    }

    private void validateSnapshotData(String dbName, String tableName) throws Exception {
        List<String> expected =
                Stream.of(
                                "CreateTableEvent{tableId=%s.%s, schema=columns={`id` INT NOT NULL,`name` VARCHAR(17),`age` INT}, primaryKeys=id, options=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1008, Alice, 21], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1009, Bob, 20], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1010, Carol, 19], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1011, Derrida, 18], op=INSERT, meta=()}")
                        .map(s -> String.format(s, dbName, tableName))
                        .collect(Collectors.toList());

        validateResult(expected, taskManagerConsumer);
    }

    private void waitForIncrementalStage(String dbName, String tableName, Statement stmt)
            throws Exception {
        stmt.execute("INSERT INTO members VALUES (0, '__fence__', 0);");

        // Ensure we change schema after incremental stage
        waitUntilSpecificEvent(
                String.format(
                        "DataChangeEvent{tableId=%s.%s, before=[], after=[0, __fence__, 0], op=INSERT, meta=()}",
                        dbName, tableName),
                taskManagerConsumer);
    }

    private void validateResult(List<String> expectedEvents, ToStringConsumer consumer)
            throws Exception {
        for (String event : expectedEvents) {
            waitUntilSpecificEvent(event, consumer);
        }
    }

    private void assertNotExists(List<String> unexpectedEvents, ToStringConsumer consumer) {
        String consumerLog = consumer.toUtf8String();
        for (String event : unexpectedEvents) {
            Assert.assertFalse(
                    consumerLog.contains(
                            String.format(event, schemaEvolveDatabase.getDatabaseName())));
        }
    }

    private void waitUntilSpecificEvent(String event, ToStringConsumer consumer) throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + SchemaEvolveE2eITCase.EVENT_WAITING_TIMEOUT;
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = consumer.toUtf8String();
            if (stdout.contains(event + "\n")) {
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
                            + consumer.toUtf8String());
        }
    }
}
