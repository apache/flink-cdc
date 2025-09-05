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

package org.apache.flink.cdc.pipeline.tests.migration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.apache.flink.cdc.pipeline.tests.utils.TarballFetcher;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.function.Function;

/** E2e cases for stopping & restarting jobs from previous state. */
class YamlJobMigrationITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(YamlJobMigrationITCase.class);

    protected UniqueDatabase mysqlInventoryDatabase;
    private final Function<String, String> dbNameFormatter =
            (s) -> String.format(s, mysqlInventoryDatabase.getDatabaseName());

    @BeforeEach
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase =
                new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
        mysqlInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        if (mysqlInventoryDatabase != null) {
            mysqlInventoryDatabase.dropDatabase();
        }
    }

    @Test
    void testBasicJobSubmitting() throws Exception {
        String content =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n",
                        parallelism);
        JobID jobID = submitPipelineJob(content);
        Assertions.assertThat(jobID).isNotNull();
        LOG.info("Submitted Job ID is {} ", jobID);

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, ], after=[2, x], op=UPDATE, meta=()}");
        LOG.info("Snapshot phase successfully finished.");

        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Job gracefully stopped.");
    }

    @ParameterizedTest(name = "{0} -> SNAPSHOT")
    @EnumSource(names = {"V3_2_1", "V3_3_0", "SNAPSHOT"})
    void testStartingJobFromSavepoint(TarballFetcher.CdcVersion migrateFromVersion)
            throws Exception {
        TarballFetcher.fetch(jobManager, migrateFromVersion);
        LOG.info("Successfully fetched CDC {}.", migrateFromVersion);

        String content =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
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
                        MySqlContainer.MYSQL_PORT,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        parallelism);
        JobID jobID = submitPipelineJob(migrateFromVersion, content);
        Assertions.assertThat(jobID).isNotNull();
        LOG.info("Submitted Job ID is {} ", jobID);

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, user_3, Shanghai, 123567891234], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, user_1, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, user_4, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, user_2, Shanghai, 123567891234], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null, null, null], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3, null, null, null], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null, null, null], op=INSERT, meta=()}");
        LOG.info("Snapshot stage finished successfully.");

        generateIncrementalEventsPhaseOne();
        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[107, rocks, box of assorted rocks, 5.3, null, null, null], after=[107, rocks, box of assorted rocks, 5.1, null, null, null], op=UPDATE, meta=()}");
        LOG.info("Incremental stage 1 finished successfully.");

        generateIncrementalEventsPhaseTwo();
        validateResult(
                dbNameFormatter,
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`new_col` INT, position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], op=INSERT, meta=()}");
        LOG.info("Incremental stage 2 finished successfully.");

        String savepointPath = stopJobWithSavepoint(jobID);
        LOG.info("Stopped Job {} and created a savepoint at {}.", jobID, savepointPath);

        JobID newJobID = submitPipelineJob(content, savepointPath, true);
        LOG.info("Reincarnated Job {} has been submitted successfully.", newJobID);

        generateIncrementalEventsPhaseThree();
        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.products, before=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], after=[110, jacket, new water resistent white wind breaker, 0.5, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], after=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], after=[], op=DELETE, meta=()}");
        cancelJob(newJobID);
    }

    @ParameterizedTest(name = "{0} -> SNAPSHOT")
    @EnumSource(names = {"SNAPSHOT"})
    void testStartingJobFromSavepointWithSchemaChange(TarballFetcher.CdcVersion migrateFromVersion)
            throws Exception {
        TarballFetcher.fetch(jobManager, migrateFromVersion);
        runInContainerAsRoot(jobManager, "chmod", "0777", "-R", "/tmp/cdc/");

        LOG.info("Successfully fetched CDC {}.", migrateFromVersion);

        String content =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  materialized.in.memory: false"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "use.legacy.json.format: true\n",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MySqlContainer.MYSQL_PORT,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        4);
        JobID jobID = submitPipelineJob(migrateFromVersion, content);
        Assertions.assertThat(jobID).isNotNull();
        LOG.info("Submitted Job ID is {} ", jobID);

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, user_1, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, user_2, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, user_3, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, user_4, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null, null, null], op=INSERT, meta=()}");
        LOG.info("Snapshot stage finished successfully.");

        generateIncrementalEventsPhaseOne();
        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[107, rocks, box of assorted rocks, 5.3, null, null, null], after=[107, rocks, box of assorted rocks, 5.1, null, null, null], op=UPDATE, meta=()}");
        LOG.info("Incremental stage 1 finished successfully.");

        String savepointPath = stopJobWithSavepoint(jobID);
        LOG.info("Stopped Job {} and created a savepoint at {}.", jobID, savepointPath);
        // Modify schema and make some data changes.
        generateIncrementalEventsPhaseTwo();
        JobID newJobID = submitPipelineJob(content, savepointPath, true);
        LOG.info("Reincarnated Job {} has been submitted successfully.", newJobID);
        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[107, rocks, box of assorted rocks, 5.3, null, null, null], after=[107, rocks, box of assorted rocks, 5.1, null, null, null], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`new_col` INT, position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], op=INSERT, meta=()}");
        LOG.info("Incremental stage 2 finished successfully.");

        generateIncrementalEventsPhaseThree();
        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.products, before=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], after=[110, jacket, new water resistent white wind breaker, 0.5, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], after=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], after=[], op=DELETE, meta=()}");
        cancelJob(newJobID);
    }

    private void generateIncrementalEventsPhaseOne() {
        executeMySqlStatements(
                mysqlInventoryDatabase,
                "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;",
                "UPDATE products SET weight='5.1' WHERE id=107;");
    }

    private void generateIncrementalEventsPhaseTwo() {
        executeMySqlStatements(
                mysqlInventoryDatabase,
                "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;",
                "UPDATE products SET weight='5.1' WHERE id=107;",
                "ALTER TABLE products ADD COLUMN new_col INT;",
                "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);",
                "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);");
    }

    private void generateIncrementalEventsPhaseThree() {
        executeMySqlStatements(
                mysqlInventoryDatabase,
                "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;",
                "UPDATE products SET weight='5.17' WHERE id=111;",
                "DELETE FROM products WHERE id=111;");
    }

    private void executeMySqlStatements(UniqueDatabase database, String... statements) {
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), database.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            for (String sql : statements) {
                try {
                    stat.execute(sql);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to execute SQL statement " + sql, e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute MySQL statements.", e);
        }
    }
}
