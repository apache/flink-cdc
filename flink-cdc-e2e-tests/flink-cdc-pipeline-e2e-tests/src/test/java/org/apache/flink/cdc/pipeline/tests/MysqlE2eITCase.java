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

import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.function.Function;

/** End-to-end tests for mysql cdc pipeline job. */
class MysqlE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlE2eITCase.class);

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    private final Function<String, String> dbNameFormatter =
            (s) -> String.format(s, mysqlInventoryDatabase.getDatabaseName());

    @BeforeEach
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
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
                        mysqlInventoryDatabase.getDatabaseName(),
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, user_4, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, user_3, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, user_2, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, user_1, Shanghai, 123567891234], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            // Perform DDL changes after the binlog is generated
            waitUntilSpecificEvent(
                    String.format(
                            "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
                            mysqlInventoryDatabase.getDatabaseName()));

            // modify table schema
            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);"); // 110
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);"); // 111
            stat.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products WHERE id=111;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[107, rocks, box of assorted rocks, 5.3, null, null, null], after=[107, rocks, box of assorted rocks, 5.1, null, null, null], op=UPDATE, meta=()}",
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`new_col` INT, position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], after=[110, jacket, new water resistent white wind breaker, 0.5, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], after=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testSyncWholeDatabaseInBatchMode() throws Exception {
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
                        mysqlInventoryDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, user_4, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, user_3, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, user_2, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, user_1, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}");
    }

    @Test
    void testSchemaChangeEvents() throws Exception {
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
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512)}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, user_4, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, user_3, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, user_2, Shanghai, 123567891234], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, user_1, Shanghai, 123567891234], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}], op=INSERT, meta=()}");

        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            // Perform DDL changes after the binlog is generated
            waitUntilSpecificEvent(
                    String.format(
                            "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
                            mysqlInventoryDatabase.getDatabaseName()));

            LOG.info("Begin schema evolution stage.");

            // Test AddColumnEvent
            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);"); // 110
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);"); // 111
            stat.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products WHERE id=111;");

            // Test AlterColumnTypeEvent
            stat.execute("ALTER TABLE products MODIFY COLUMN new_col BIGINT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'derrida','forever 21',2.1728, null, null, null, 2147483649);"); // 112

            // Test RenameColumnEvent
            stat.execute("ALTER TABLE products RENAME COLUMN new_col TO new_column;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'dynazenon','SSSS',2.1728, null, null, null, 2147483649);"); // 113

            // Test DropColumnEvent
            stat.execute("ALTER TABLE products DROP COLUMN new_column;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'evangelion','Eva',2.1728, null, null, null);"); // 114

            // Test Batch AddColumnEvent
            stat.execute(
                    "ALTER TABLE products ADD COLUMN (batch_new_col1 INT,batch_new_col2 INT);");
            stat.execute(
                    "ALTER TABLE products ADD COLUMN batch_new_col3 INT,ADD COLUMN batch_new_col4 INT after batch_new_col3;");

            // Test TruncateTableEvent
            stat.execute("TRUNCATE TABLE products;");

            // Test DropTableEvent. It's all over.
            stat.execute("DROP TABLE products;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[107, rocks, box of assorted rocks, 5.3, null, null, null], after=[107, rocks, box of assorted rocks, 5.1, null, null, null], op=UPDATE, meta=()}",
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`new_col` INT, position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1], after=[110, jacket, new water resistent white wind breaker, 0.5, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1], after=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1], after=[], op=DELETE, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.products, typeMapping={new_col=BIGINT}, oldTypeMapping={new_col=INT}}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[112, derrida, forever 21, 2.1728, null, null, null, 2147483649], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=%s.products, nameMapping={new_col=new_column}}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[113, dynazenon, SSSS, 2.1728, null, null, null, 2147483649], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.products, droppedColumnNames=[new_column]}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[114, evangelion, Eva, 2.1728, null, null, null], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`batch_new_col1` INT, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`batch_new_col2` INT, position=LAST, existedColumnName=null}]}",
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`batch_new_col3` INT, position=LAST, existedColumnName=null}]}",
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`batch_new_col4` INT, position=AFTER, existedColumnName=batch_new_col3}]}",
                "TruncateTableEvent{tableId=%s.products}",
                "DropTableEvent{tableId=%s.products}");
    }

    @Test
    void testSoftDelete() throws Exception {
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
                                + "transform:\n"
                                + "  - source-table: \\.*.\\.*\n"
                                + "    projection: \\*, __data_event_type__ AS op_type\n"
                                + "    converter-after-transform: SOFT_DELETE\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.customers, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`address` VARCHAR(1024),`phone_number` VARCHAR(512),`op_type` STRING NOT NULL}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[104, user_4, Shanghai, 123567891234, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[103, user_3, Shanghai, 123567891234, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[102, user_2, Shanghai, 123567891234, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.customers, before=[], after=[101, user_1, Shanghai, 123567891234, +I], op=INSERT, meta=()}",
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING,`op_type` STRING NOT NULL}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null, null, null, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[107, rocks, box of assorted rocks, 5.3, null, null, null, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null, null, null, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, red, {\"k1\": \"v1\", \"k2\": \"v2\"}, {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, red, {\"key3\": \"value3\"}, {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, white, {\"key4\": \"value4\"}, {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, red, {\"key1\": \"value1\"}, {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[102, car battery, 12V car battery, 8.1, white, {\"key2\": \"value2\"}, {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}, +I], op=INSERT, meta=()}");

        LOG.info("Begin incremental reading stage.");
        // generate binlogs
        String mysqlJdbcUrl =
                String.format(
                        "jdbc:mysql://%s:%s/%s",
                        MYSQL.getHost(),
                        MYSQL.getDatabasePort(),
                        mysqlInventoryDatabase.getDatabaseName());
        try (Connection conn =
                        DriverManager.getConnection(
                                mysqlJdbcUrl, MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);
                Statement stat = conn.createStatement()) {
            stat.execute("UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            stat.execute("UPDATE products SET weight='5.1' WHERE id=107;");

            // Perform DDL changes after the binlog is generated
            waitUntilSpecificEvent(
                    String.format(
                            "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null, -U], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null, +U], op=UPDATE, meta=()}",
                            mysqlInventoryDatabase.getDatabaseName()));

            LOG.info("Begin schema evolution stage.");

            // Test AddColumnEvent
            stat.execute("ALTER TABLE products ADD COLUMN new_col INT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2, null, null, null, 1);"); // 110
            stat.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18, null, null, null, 1);"); // 111
            stat.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            stat.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            stat.execute("DELETE FROM products WHERE id=111;");

            // Test AlterColumnTypeEvent
            stat.execute("ALTER TABLE products MODIFY COLUMN new_col BIGINT;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'derrida','forever 21',2.1728, null, null, null, 2147483649);"); // 112

            // Test RenameColumnEvent
            stat.execute("ALTER TABLE products RENAME COLUMN new_col TO new_column;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'dynazenon','SSSS',2.1728, null, null, null, 2147483649);"); // 113

            // Test DropColumnEvent
            stat.execute("ALTER TABLE products DROP COLUMN new_column;");
            stat.execute(
                    "INSERT INTO products VALUES (default,'evangelion','Eva',2.1728, null, null, null);"); // 114

            // Test TruncateTableEvent
            stat.execute("TRUNCATE TABLE products;");

            // Test DropTableEvent. It's all over.
            stat.execute("DROP TABLE products;");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        validateResult(
                dbNameFormatter,
                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null, -U], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null, +U], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[107, rocks, box of assorted rocks, 5.3, null, null, null, -U], after=[107, rocks, box of assorted rocks, 5.1, null, null, null, +U], op=UPDATE, meta=()}",
                "AddColumnEvent{tableId=%s.products, addedColumns=[ColumnWithPosition{column=`new_col` INT, position=AFTER, existedColumnName=point_c}]}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1, +I], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[110, jacket, water resistent white wind breaker, 0.2, null, null, null, 1, -U], after=[110, jacket, new water resistent white wind breaker, 0.5, null, null, null, 1, +U], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[111, scooter, Big 2-wheel scooter , 5.18, null, null, null, 1, -U], after=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1, +U], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.17, null, null, null, 1, -D], op=INSERT, meta=()}",
                "AlterColumnTypeEvent{tableId=%s.products, typeMapping={new_col=BIGINT}, oldTypeMapping={new_col=INT}}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[112, derrida, forever 21, 2.1728, null, null, null, 2147483649, +I], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=%s.products, nameMapping={new_col=new_column}}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[113, dynazenon, SSSS, 2.1728, null, null, null, 2147483649, +I], op=INSERT, meta=()}",
                "DropColumnEvent{tableId=%s.products, droppedColumnNames=[new_column]}",
                "DataChangeEvent{tableId=%s.products, before=[], after=[114, evangelion, Eva, 2.1728, null, null, null, +I], op=INSERT, meta=()}",
                "TruncateTableEvent{tableId=%s.products}",
                "DropTableEvent{tableId=%s.products}");
    }

    @Test
    void testDanglingDropTableEventInBinlog() throws Exception {
        // Create a new table for later deletion
        try (Connection connection = mysqlInventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE live_fast(ID INT PRIMARY KEY);");
        }

        String logFileName = null;
        Long logPosition = null;

        try (Connection connection = mysqlInventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW BINARY LOGS;");
            while (rs.next()) {
                logFileName = rs.getString("Log_name");
                logPosition = rs.getLong("File_size");
            }
        }

        // We start reading binlog from the tail of current position and file to avoid reading
        // previous events. The next DDL event (DROP TABLE) will push binlog position forward.
        Preconditions.checkNotNull(logFileName, "Log file name must not be null");
        Preconditions.checkNotNull(logPosition, "Log position name must not be null");
        LOG.info("Trying to restore from {} @ {}...", logFileName, logPosition);

        try (Connection connection = mysqlInventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE live_fast;");
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
                                + "  scan.startup.mode: specific-offset\n"
                                + "  scan.startup.specific-offset.file: %s\n"
                                + "  scan.startup.specific-offset.pos: %d\n"
                                + "  scan.binlog.newly-added-table.enabled: true\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  schema.change.behavior: evolve",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        mysqlInventoryDatabase.getDatabaseName(),
                        logFileName,
                        logPosition,
                        parallelism);
        submitPipelineJob(pipelineJob);
        waitUntilJobRunning(Duration.ofSeconds(30));
        try (Connection connection = mysqlInventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
        }
        validateResult(
                dbNameFormatter,
                "CreateTableEvent{tableId=%s.products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT,`enum_c` STRING 'red',`json_c` STRING,`point_c` STRING}, primaryKeys=id, options=()}",
                "DataChangeEvent{tableId=%s.products, before=[106, hammer, 16oz carpenter's hammer, 1.0, null, null, null], after=[106, hammer, 18oz carpenter hammer, 1.0, null, null, null], op=UPDATE, meta=()}");
    }
}
