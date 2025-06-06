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
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;

import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Function;

/** End-to-end tests for postgres cdc pipeline job. */
public class PostgresE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresE2eITCase.class);

    @Container
    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withDatabaseName(INTER_CONTAINER_POSTGRES_ALIAS)
                    .withUsername(POSTGRES_TEST_USER)
                    .withPassword(POSTGRES_TEST_PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical");

    private final UniqueDatabase postgresInventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "postgres_inventory",
                    POSTGRES_TEST_USER,
                    POSTGRES_TEST_PASSWORD);

    private final Function<String, String> dbNameFormatter =
            (s) -> String.format(s, postgresInventoryDatabase.getDatabaseName());

    @BeforeEach
    public void before() throws Exception {
        super.before();
        postgresInventoryDatabase.createAndInitialize();
    }

    @AfterEach
    public void after() {
        super.after();
        postgresInventoryDatabase.dropDatabase();
    }

    @Test
    void testSyncWholeDatabase() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: postgres\n"
                                + "  hostname: %s\n"
                                + "  port: %d\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.*.\\.*\n"
                                + "  slot.name: flink-test\n"
                                //                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_POSTGRES_ALIAS,
                        postgresInventoryDatabase.getDatabasePort(),
                        POSTGRES_TEST_USER,
                        POSTGRES_TEST_PASSWORD,
                        postgresInventoryDatabase.getDatabaseName(),
                        parallelism);
        Path postgresCdcJar = TestUtils.getResource("postgres-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, postgresCdcJar);
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
    }
}
