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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link org.apache.flink.cdc.connectors.mysql.source.MySqlDataSource}. */
public class MySqlMultipleSourceITCase extends MySqlSourceTestBase {

    protected static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL8_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);
    private final UniqueDatabase fullTypesMySql57Database =
            new UniqueDatabase(
                    MYSQL_CONTAINER,
                    "column_type_test",
                    MySqSourceTestUtils.TEST_USER,
                    MySqSourceTestUtils.TEST_PASSWORD);

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private static final PrintStream standardOut = System.out;
    private static final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setOut(new PrintStream(outCaptor));
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL8_CONTAINER.stop();
        MYSQL_CONTAINER.stop();
        LOG.info("Containers are stopped.");
        System.setOut(standardOut);
    }

    @Test
    public void testSingleSplitMultipleSources() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        inventoryDatabase.createAndInitialize();
        fullTypesMySql57Database.createAndInitialize();
        // Take over STDOUT as we need to check the output of values sink

        // Setup value source
        Configuration sourceConfig1 = new Configuration();
        sourceConfig1.set(MySqlDataSourceOptions.PORT, MYSQL_CONTAINER.getDatabasePort());
        sourceConfig1.set(MySqlDataSourceOptions.HOSTNAME, MYSQL_CONTAINER.getHost());
        sourceConfig1.set(MySqlDataSourceOptions.TABLES, "column_type_test_\\.*.precision_types");
        sourceConfig1.set(MySqlDataSourceOptions.SERVER_ID, "52300");
        sourceConfig1.set(MySqlDataSourceOptions.USERNAME, MYSQL_CONTAINER.getUsername());
        sourceConfig1.set(MySqlDataSourceOptions.PASSWORD, MYSQL_CONTAINER.getPassword());
        sourceConfig1.set(MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED, true);
        sourceConfig1.set(MySqlDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfig1.set(MySqlDataSourceOptions.SCAN_STARTUP_MODE, "initial");

        Configuration sourceConfig2 = new Configuration();
        sourceConfig2.set(MySqlDataSourceOptions.PORT, MYSQL8_CONTAINER.getDatabasePort());
        sourceConfig2.set(MySqlDataSourceOptions.HOSTNAME, MYSQL8_CONTAINER.getHost());
        sourceConfig2.set(MySqlDataSourceOptions.TABLES, "inventory_\\.*.products");
        sourceConfig2.set(MySqlDataSourceOptions.SERVER_ID, "52300");
        sourceConfig2.set(MySqlDataSourceOptions.USERNAME, MYSQL8_CONTAINER.getUsername());
        sourceConfig2.set(MySqlDataSourceOptions.PASSWORD, MYSQL8_CONTAINER.getPassword());
        sourceConfig2.set(MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED, true);
        sourceConfig2.set(MySqlDataSourceOptions.SERVER_TIME_ZONE, "UTC");
        sourceConfig2.set(MySqlDataSourceOptions.SCAN_STARTUP_MODE, "initial");
        SourceDef sourceDef1 =
                new SourceDef(MySqlDataSourceFactory.IDENTIFIER, "mysql Source1", sourceConfig1);
        SourceDef sourceDef2 =
                new SourceDef(MySqlDataSourceFactory.IDENTIFIER, "mysql Source2", sourceConfig2);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.LENIENT);
        List<SourceDef> sourceDefs = new ArrayList<>();
        sourceDefs.add(sourceDef1);
        sourceDefs.add(sourceDef2);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDefs,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                execution.execute();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
        thread.start();
        Thread.sleep(60000);
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`products` ADD COLUMN `cols5` BIT NULL;",
                            inventoryDatabase.getDatabaseName()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try (Connection connection = fullTypesMySql57Database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`precision_types` ADD COLUMN `cols5` BIT NULL;",
                            fullTypesMySql57Database.getDatabaseName()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Thread.sleep(60000);
        List<String> table1Results = ValuesDatabase.getAllResults();
        Collections.sort(table1Results);
        List<String> outputEvents = Arrays.asList(outCaptor.toString().trim().split("\n"));
        outputEvents =
                outputEvents.stream().map(s -> s.replaceAll("\r", "")).collect(Collectors.toList());
        Collections.sort(outputEvents);
        assertThat(table1Results)
                .containsExactly(
                        fullTypesMySql57Database.getDatabaseName()
                                + ".precision_types:id=1;decimal_c0=123.40;decimal_c1=1234.5000;decimal_c2=1234.5600;time_c=18:00;time_3_c=18:00:22.100;time_6_c=18:00:22.100;datetime_c=2020-07-17T18:00;datetime3_c=2020-07-17T18:00:22;datetime6_c=2020-07-17T18:00:22;timestamp_c=2020-07-17T18:00;timestamp3_c=2020-07-17T18:00:22;timestamp6_c=2020-07-17T18:00:22;float_c0=2.0;float_c1=3.0;float_c2=5.0;real_c0=7.0;real_c1=11.0;real_c2=13.0;double_c0=17.0;double_c1=19.0;double_c2=23.0;double_precision_c0=29.0;double_precision_c1=31.0;double_precision_c2=37.0;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=101;name=scooter;description=Small 2-wheel scooter;weight=3.14;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=102;name=car battery;description=12V car battery;weight=8.1;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=103;name=12-pack drill bits;description=12-pack of drill bits with sizes ranging from #40 to #3;weight=0.8;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=104;name=hammer;description=12oz carpenter's hammer;weight=0.75;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=105;name=hammer;description=14oz carpenter's hammer;weight=0.875;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=106;name=hammer;description=16oz carpenter's hammer;weight=1.0;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=107;name=rocks;description=box of assorted rocks;weight=5.3;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=108;name=jacket;description=water resistent black wind breaker;weight=0.1;cols5=",
                        inventoryDatabase.getDatabaseName()
                                + ".products:id=109;name=spare tire;description=24 inch spare tire;weight=22.2;cols5=");

        assertThat(outputEvents)
                .containsExactly(
                        "AddColumnEvent{tableId="
                                + fullTypesMySql57Database.getDatabaseName()
                                + ".precision_types, addedColumns=[ColumnWithPosition{column=`cols5` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, addedColumns=[ColumnWithPosition{column=`cols5` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "CreateTableEvent{tableId="
                                + fullTypesMySql57Database.getDatabaseName()
                                + ".precision_types, schema=columns={`id` DECIMAL(20, 0) NOT NULL,`decimal_c0` DECIMAL(6, 2),`decimal_c1` DECIMAL(9, 4),`decimal_c2` DECIMAL(20, 4),`time_c` TIME(0),`time_3_c` TIME(3),`time_6_c` TIME(6),`datetime_c` TIMESTAMP(0),`datetime3_c` TIMESTAMP(3),`datetime6_c` TIMESTAMP(6),`timestamp_c` TIMESTAMP_LTZ(0),`timestamp3_c` TIMESTAMP_LTZ(3),`timestamp6_c` TIMESTAMP_LTZ(6),`float_c0` DOUBLE,`float_c1` DOUBLE,`float_c2` DOUBLE,`real_c0` DOUBLE,`real_c1` DOUBLE,`real_c2` DOUBLE,`double_c0` DOUBLE,`double_c1` DOUBLE,`double_c2` DOUBLE,`double_precision_c0` DOUBLE,`double_precision_c1` DOUBLE,`double_precision_c2` DOUBLE}, primaryKeys=id, options=()}",
                        "CreateTableEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, schema=columns={`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL 'flink',`description` VARCHAR(512),`weight` FLOAT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId="
                                + fullTypesMySql57Database.getDatabaseName()
                                + ".precision_types, before=[], after=[1, 123.40, 1234.5000, 1234.5600, 18:00, 18:00:22.100, 18:00:22.100, 2020-07-17T18:00, 2020-07-17T18:00:22, 2020-07-17T18:00:22, 2020-07-17T18:00, 2020-07-17T18:00:22, 2020-07-17T18:00:22, 2.0, 3.0, 5.0, 7.0, 11.0, 13.0, 17.0, 19.0, 23.0, 29.0, 31.0, 37.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[102, car battery, 12V car battery, 8.1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[107, rocks, box of assorted rocks, 5.3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[108, jacket, water resistent black wind breaker, 0.1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId="
                                + inventoryDatabase.getDatabaseName()
                                + ".products, before=[], after=[109, spare tire, 24 inch spare tire, 22.2], op=INSERT, meta=()}");

        thread.interrupt();
    }
}
