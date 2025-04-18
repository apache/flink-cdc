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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.junit.jupiter.Container;

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
class SchemaEvolvingTransformE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG =
            LoggerFactory.getLogger(SchemaEvolvingTransformE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";
    protected static final long EVENT_WAITING_TIMEOUT = 60000L;

    @Container
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

    @BeforeEach
    public void before() throws Exception {
        super.before();
        schemaEvolveDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        schemaEvolveDatabase.dropDatabase();
    }

    @Test
    void testSchemaEvolve() throws Exception {
        testGenericSchemaEvolution(
                "evolve",
                false,
                false,
                false,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012 -> Eve, 1012, Eve, 17, 0, 1024144, age < 20], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}",
                        "DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013 -> Fiona, 1013, Fiona, 16.0, 1026169, age < 20], op=INSERT, meta=()}",
                        "TruncateTableEvent{tableId=%s.members}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014 -> Gem, 1014, Gem, 17.0, 1028196, age < 20], op=INSERT, meta=()}",
                        "DropTableEvent{tableId=%s.members}"));
    }

    @Test
    void testSchemaEvolveWithIncompatibleChanges() throws Exception {
        testGenericSchemaEvolution(
                "evolve",
                true,
                false,
                false,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.merged, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=tag}]}",
                        "DataChangeEvent{tableId=%s.merged, before=[], after=[1012 -> Eve, 1012, Eve, 17, 1024144, age < 20, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.merged, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "AddColumnEvent{tableId=%s.merged, addedColumns=[ColumnWithPosition{column=`biological_sex` TINYINT, position=AFTER, existedColumnName=gender}]}",
                        "DataChangeEvent{tableId=%s.merged, before=[], after=[1013 -> Fiona, 1013, Fiona, 16.0, 1026169, age < 20, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.merged, before=[], after=[1014 -> Gem, 1014, Gem, 17.0, 1028196, age < 20, null, null], op=INSERT, meta=()}"));
    }

    @Test
    void testSchemaEvolveWithException() throws Exception {
        testGenericSchemaEvolution(
                "evolve",
                false,
                true,
                false,
                Collections.emptyList(),
                Arrays.asList(
                        "An exception was triggered from Schema change applying task. Job will fail now.",
                        "org.apache.flink.util.FlinkRuntimeException: Failed to apply schema change event.",
                        "Caused by: org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException",
                        "org.apache.flink.runtime.JobException: Recovery is suppressed by NoRestartBackoffTimeStrategy"));
    }

    @Test
    void testSchemaTryEvolveWithException() throws Exception {
        testGenericSchemaEvolution(
                "try_evolve",
                false,
                true,
                false,
                Arrays.asList(
                        // Add column never succeeded, so age column will not appear.
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012 -> Eve, 1012, Eve, 17, 1024144, age < 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013 -> Fiona, 1013, Fiona, null, 1026169, age < 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014 -> Gem, 1014, Gem, null, 1028196, age < 20], op=INSERT, meta=()}"),
                Arrays.asList(
                        "Failed to apply schema change AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change TruncateTableEvent{tableId=%s.members}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=TruncateTableEvent{tableId=%s.members}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}",
                        "Failed to apply schema change DropTableEvent{tableId=%s.members}, but keeps running in tolerant mode. Caused by: UnsupportedSchemaChangeEventException{applyingEvent=DropTableEvent{tableId=%s.members}, exceptionMessage='Rejected schema change event since error.on.schema.change is enabled.', cause='null'}"));
    }

    @Test
    void testSchemaIgnore() throws Exception {

        testGenericSchemaEvolution(
                "ignore",
                false,
                false,
                false,
                Arrays.asList(
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012 -> Eve, 1012, Eve, 17, 1024144, age < 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013 -> Fiona, 1013, Fiona, null, 1026169, age < 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014 -> Gem, 1014, Gem, null, 1028196, age < 20], op=INSERT, meta=()}"));
    }

    @Test
    void testSchemaException() throws Exception {
        testGenericSchemaEvolution(
                "exception",
                false,
                false,
                false,
                Collections.emptyList(),
                Arrays.asList(
                        "An exception was triggered from Schema change applying task. Job will fail now.",
                        "org.apache.flink.util.FlinkRuntimeException: Failed to apply schema change event."));
    }

    @Test
    void testLenientSchemaEvolution() throws Exception {
        testGenericSchemaEvolution(
                "lenient",
                false,
                false,
                false,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012 -> Eve, 1012, Eve, 17, 1024144, age < 20, 0], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`biological_sex` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013 -> Fiona, 1013, Fiona, 16.0, 1026169, age < 20, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014 -> Gem, 1014, Gem, 17.0, 1028196, age < 20, null, null], op=INSERT, meta=()}"));
    }

    @Test
    void testFineGrainedSchemaEvolution() throws Exception {

        testGenericSchemaEvolution(
                "evolve",
                false,
                false,
                true,
                Arrays.asList(
                        "AddColumnEvent{tableId=%s.members, addedColumns=[ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1012 -> Eve, 1012, Eve, 17, 0, 1024144, age < 20], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=%s.members, typeMapping={age=DOUBLE}, oldTypeMapping={age=INT}}",
                        "RenameColumnEvent{tableId=%s.members, nameMapping={gender=biological_sex}}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1013 -> Fiona, 1013, Fiona, 16.0, null, 1026169, age < 20], op=INSERT, meta=()}",
                        "TruncateTableEvent{tableId=%s.members}",
                        "DataChangeEvent{tableId=%s.members, before=[], after=[1014 -> Gem, 1014, Gem, 17.0, null, 1028196, age < 20], op=INSERT, meta=()}"),
                Arrays.asList(
                        "Ignored schema change DropColumnEvent{tableId=%s.members, droppedColumnNames=[biological_sex]}.",
                        "Ignored schema change DropTableEvent{tableId=%s.members}."));
    }

    @Test
    void testUnexpectedBehavior() {
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
        Assertions.assertThatThrownBy(
                        () ->
                                submitPipelineJob(
                                        pipelineJob, mysqlCdcJar, valuesCdcJar, mysqlDriverJar))
                .isExactlyInstanceOf(AssertionError.class);
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
                                + (triggerError ? "  error.on.schema.change: true\n" : "\n")
                                + "transform:\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    projection: CAST(id AS VARCHAR) || ' -> ' || name AS uid, *, id * id AS id_square, 'age < 20' as tag\n"
                                + "    filter: age < 20\n"
                                + "  - source-table: %s.\\.*\n"
                                + "    projection: CAST(id AS VARCHAR) || ' -> ' || name AS uid, *, 0 - id * id AS id_square, 'age >= 20' as tag\n"
                                + "    filter: age >= 20\n"
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
                        dbName,
                        dbName,
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

            // triggers AlterColumnTypeEvent
            stmt.execute("ALTER TABLE members MODIFY COLUMN age DOUBLE;");

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
                                "CreateTableEvent{tableId=%s.%s, schema=columns={`uid` STRING,`id` INT NOT NULL,`name` VARCHAR(17),`age` INT,`id_square` INT,`tag` STRING}, primaryKeys=id, options=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1009 -> Bob, 1009, Bob, 20, -1018081, age >= 20], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1008 -> Alice, 1008, Alice, 21, -1016064, age >= 20], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1011 -> Derrida, 1011, Derrida, 18, 1022121, age < 20], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[1010 -> Carol, 1010, Carol, 19, 1020100, age < 20], op=INSERT, meta=()}")
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
                        "DataChangeEvent{tableId=%s.%s, before=[], after=[0 -> __fence__, 0, __fence__, 0, 0, age < 20], op=INSERT, meta=()}",
                        dbName, tableName),
                taskManagerConsumer);
    }

    private void validateResult(List<String> expectedEvents, ToStringConsumer consumer)
            throws Exception {
        for (String event : expectedEvents) {
            waitUntilSpecificEvent(event, consumer);
        }
    }

    private void waitUntilSpecificEvent(String event, ToStringConsumer consumer) throws Exception {
        boolean result = false;
        long endTimeout =
                System.currentTimeMillis() + SchemaEvolvingTransformE2eITCase.EVENT_WAITING_TIMEOUT;
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
