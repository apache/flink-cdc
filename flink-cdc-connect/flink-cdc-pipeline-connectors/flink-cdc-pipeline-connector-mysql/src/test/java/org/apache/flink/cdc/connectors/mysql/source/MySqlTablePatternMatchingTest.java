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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.loopCheck;

/** Test cases for matching MySQL source tables. */
class MySqlTablePatternMatchingTest extends MySqlSourceTestBase {

    private final PrintStream standardOut = System.out;

    private static final List<Tuple2<String, String>> TEST_TABLES =
            Arrays.asList(
                    Tuple2.of("db", "tbl1"),
                    Tuple2.of("db", "tbl2"),
                    Tuple2.of("db", "tbl3"),
                    Tuple2.of("db", "tbl4"),
                    Tuple2.of("db2", "tbl2"),
                    Tuple2.of("db3", "tbl3"),
                    Tuple2.of("db4", "tbl4"));

    @BeforeAll
    static void initializeDatabase() {
        initializeMySqlTables(TEST_TABLES);
    }

    @AfterAll
    static void tearDownDatabase() {
        tearDownMySqlTables(TEST_TABLES);
    }

    @Test
    void testWildcardMatching() {
        Assertions.assertThat(testGenericTableMatching("\\.*.\\.*", null, false))
                .containsExactlyInAnyOrder(
                        "db.tbl1",
                        "db.tbl2",
                        "db.tbl3",
                        "db.tbl4",
                        "db2.tbl2",
                        "db3.tbl3",
                        "db4.tbl4");

        Assertions.assertThat(testGenericTableMatching("\\.*.\\.*", null, true))
                .containsExactlyInAnyOrder(".*\\..*");
    }

    @Test
    void testWildcardMatchingDatabases() {
        Assertions.assertThat(testGenericTableMatching("\\.*.tbl[3-4]", null, false))
                .containsExactlyInAnyOrder("db.tbl3", "db.tbl4", "db3.tbl3", "db4.tbl4");

        Assertions.assertThat(testGenericTableMatching("\\.*.tbl[3-4]", null, true))
                .containsExactlyInAnyOrder(".*\\.tbl[3-4]");
    }

    @Test
    void testWildcardMatchingTables() {
        Assertions.assertThat(testGenericTableMatching("db.\\.*", null, false))
                .containsExactlyInAnyOrder("db.tbl1", "db.tbl2", "db.tbl3", "db.tbl4");

        Assertions.assertThat(testGenericTableMatching("db.\\.*", null, true))
                .containsExactlyInAnyOrder("db\\..*");
    }

    @Test
    void testWildcardMatchingPartialDatabases() {
        // `db.` matches `db2`, `db3`, `db4` but not `db`
        Assertions.assertThat(testGenericTableMatching("db\\..\\.*", null, false))
                .containsExactlyInAnyOrder("db2.tbl2", "db3.tbl3", "db4.tbl4");

        Assertions.assertThat(testGenericTableMatching("db\\..\\.*", null, true))
                .containsExactlyInAnyOrder("db.\\..*");
    }

    @Test
    void testWildcardMatchingWithExclusion() {
        Assertions.assertThat(testGenericTableMatching("\\.*.\\.*", "db.tbl3", false))
                .containsExactlyInAnyOrder(
                        "db.tbl1", "db.tbl2", "db.tbl4", "db2.tbl2", "db3.tbl3", "db4.tbl4");
    }

    @Test
    void testWildcardMatchingDatabasesWithExclusion() {
        Assertions.assertThat(testGenericTableMatching("\\.*.tbl[3-4]", "db.tbl[3-4]", false))
                .containsExactlyInAnyOrder("db3.tbl3", "db4.tbl4");
    }

    @Test
    void testWildcardMatchingTablesWithExclusion() {
        Assertions.assertThat(testGenericTableMatching("db.\\.*", "db.tbl4", false))
                .containsExactlyInAnyOrder("db.tbl1", "db.tbl2", "db.tbl3");
    }

    @Test
    void testWildcardMatchingPartialDatabasesWithExclusion() {
        // `db.` matches `db2`, `db3`, `db4` but not `db`
        Assertions.assertThat(testGenericTableMatching("db\\..\\.*", "db3.\\.*", false))
                .containsExactlyInAnyOrder("db2.tbl2", "db4.tbl4");
    }

    @Test
    void testMatchingTablesWithMultipleRules() {
        Assertions.assertThat(testGenericTableMatching("db.tbl1,db2.tbl\\.*,db3.tbl3", null, false))
                .containsExactlyInAnyOrder("db.tbl1", "db2.tbl2", "db3.tbl3");

        Assertions.assertThat(testGenericTableMatching("db.tbl1,db2.tbl\\.*,db3.tbl3", null, true))
                .containsExactlyInAnyOrder("db\\.tbl1|db2\\.tbl.*|db3\\.tbl3");
    }

    @Test
    void testMatchingTablesWithSpacedRules() {
        List<String> spacedRules =
                Arrays.asList(
                        "db.tbl1, db2.tbl\\.*, db3.tbl3",
                        "db.tbl1 ,db2.tbl\\.* ,db3.tbl3",
                        "db.tbl1 , db2.tbl\\.* , db3.tbl3");

        Assertions.assertThat(spacedRules)
                .map(rule -> testGenericTableMatching(rule, null, false))
                .containsOnly(Arrays.asList("db.tbl1", "db2.tbl2", "db3.tbl3"));

        Assertions.assertThat(spacedRules)
                .map(rule -> testGenericTableMatching(rule, null, true))
                .containsOnly(Arrays.asList("db\\.tbl1|db2\\.tbl.*|db3\\.tbl3"));
    }

    @Test
    void testWildcardMatchingRealTables() throws Exception {
        String[] expected =
                new String[] {
                    "CreateTableEvent{tableId=db.tbl1, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl1, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db.tbl2, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl2, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl3, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db.tbl4, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl4, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db2.tbl2, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db2.tbl2, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db3.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db3.tbl3, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db4.tbl4, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db4.tbl4, before=[], after=[17], op=INSERT, meta=()}"
                };

        Assertions.assertThat(getRealWorldMatchedTables("\\.*.\\.*", null, false, expected.length))
                .containsExactlyInAnyOrder(expected);

        Assertions.assertThat(getRealWorldMatchedTables("\\.*.\\.*", null, true, expected.length))
                .containsExactlyInAnyOrder(expected);
    }

    @Test
    void testWildcardMatchingDatabasesRealTables() throws Exception {
        String[] expected =
                new String[] {
                    "CreateTableEvent{tableId=db.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl3, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db.tbl4, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl4, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db3.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db3.tbl3, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db4.tbl4, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db4.tbl4, before=[], after=[17], op=INSERT, meta=()}"
                };

        Assertions.assertThat(
                        getRealWorldMatchedTables("\\.*.tbl[3-4]", null, false, expected.length))
                .containsExactlyInAnyOrder(expected);

        Assertions.assertThat(
                        getRealWorldMatchedTables("\\.*.tbl[3-4]", null, true, expected.length))
                .containsExactlyInAnyOrder(expected);
    }

    @Test
    void testWildcardMatchingTablesRealTables() throws Exception {
        String[] expected =
                new String[] {
                    "CreateTableEvent{tableId=db.tbl1, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl1, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db.tbl2, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl2, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl3, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db.tbl4, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl4, before=[], after=[17], op=INSERT, meta=()}"
                };

        Assertions.assertThat(getRealWorldMatchedTables("db.\\.*", null, false, expected.length))
                .containsExactlyInAnyOrder(expected);

        Assertions.assertThat(getRealWorldMatchedTables("db.\\.*", null, true, expected.length))
                .containsExactlyInAnyOrder(expected);
    }

    @Test
    void testWildcardMatchingPartialDatabasesRealTables() throws Exception {
        String[] expected =
                new String[] {
                    "CreateTableEvent{tableId=db2.tbl2, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db2.tbl2, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db3.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db3.tbl3, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db4.tbl4, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db4.tbl4, before=[], after=[17], op=INSERT, meta=()}"
                };

        // `db.` matches `db2`, `db3`, `db4` but not `db`
        Assertions.assertThat(getRealWorldMatchedTables("db\\..\\.*", null, false, expected.length))
                .containsExactlyInAnyOrder(expected);

        Assertions.assertThat(getRealWorldMatchedTables("db\\..\\.*", null, true, expected.length))
                .containsExactlyInAnyOrder(expected);
    }

    @Test
    void testMultipleRulesWithRealTables() throws Exception {
        String[] expected =
                new String[] {
                    "CreateTableEvent{tableId=db.tbl1, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl1, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db2.tbl2, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db2.tbl2, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db3.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db3.tbl3, before=[], after=[17], op=INSERT, meta=()}"
                };

        Assertions.assertThat(
                        getRealWorldMatchedTables(
                                "db.tbl1,db2.tbl\\.*,db3.tbl3", null, false, expected.length))
                .containsExactlyInAnyOrder(expected);

        Assertions.assertThat(
                        getRealWorldMatchedTables(
                                "db.tbl1,db2.tbl\\.*,db3.tbl3", null, true, expected.length))
                .containsExactlyInAnyOrder(expected);
    }

    @Test
    void testMatchingRealTablesWithSpacedRules() throws Exception {
        String[] expected =
                new String[] {
                    "CreateTableEvent{tableId=db.tbl1, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db.tbl1, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db2.tbl2, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db2.tbl2, before=[], after=[17], op=INSERT, meta=()}",
                    "CreateTableEvent{tableId=db3.tbl3, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                    "DataChangeEvent{tableId=db3.tbl3, before=[], after=[17], op=INSERT, meta=()}"
                };

        Assertions.assertThat(
                        getRealWorldMatchedTables(
                                "db.tbl1 , db2.tbl\\.* , db3.tbl3", null, false, expected.length))
                .containsExactlyInAnyOrder(expected);

        Assertions.assertThat(
                        getRealWorldMatchedTables(
                                "db.tbl1 , db2.tbl\\.* , db3.tbl3", null, true, expected.length))
                .containsExactlyInAnyOrder(expected);
    }

    private static void initializeMySqlTables(List<Tuple2<String, String>> tableNames) {
        tableNames.forEach(
                tableName -> {
                    try (Connection connection =
                                    DriverManager.getConnection(
                                            MYSQL_CONTAINER.getJdbcUrl(),
                                            TEST_USER,
                                            TEST_PASSWORD);
                            Statement statement = connection.createStatement()) {
                        statement.execute(
                                String.format("CREATE DATABASE IF NOT EXISTS `%s`;", tableName.f0));
                        statement.execute(
                                String.format(
                                        "CREATE TABLE IF NOT EXISTS `%s`.`%s` (id INT PRIMARY KEY NOT NULL);",
                                        tableName.f0, tableName.f1));

                        statement.execute(
                                String.format(
                                        "INSERT INTO `%s`.`%s` VALUES (17);",
                                        tableName.f0, tableName.f1));
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to initialize databases and tables", e);
                    }
                });
    }

    private static void tearDownMySqlTables(List<Tuple2<String, String>> tableNames) {
        tableNames.forEach(
                tableName -> {
                    try (Connection connection =
                                    DriverManager.getConnection(
                                            MYSQL_CONTAINER.getJdbcUrl(),
                                            TEST_USER,
                                            TEST_PASSWORD);
                            Statement statement = connection.createStatement()) {
                        statement.execute(
                                String.format("DROP DATABASE IF EXISTS `%s`;", tableName.f0));
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to clean-up databases", e);
                    }
                });
    }

    private List<String> testGenericTableMatching(
            String tablesConfig,
            @Nullable String tablesExclude,
            boolean scanBinlogNewlyAddedTable) {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), tablesConfig);
        options.put(
                SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED.key(),
                String.valueOf(scanBinlogNewlyAddedTable));
        if (tablesExclude != null) {
            options.put(TABLES_EXCLUDE.key(), tablesExclude);
        }
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        return dataSource.getSourceConfig().getTableList();
    }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }

    private List<String> getRealWorldMatchedTables(
            String tables,
            @Nullable String tablesExclude,
            boolean scanBinlogNewlyAddedTable,
            int expectedCount)
            throws Exception {
        try (ByteArrayOutputStream outCaptor = new ByteArrayOutputStream()) {
            System.setOut(new PrintStream(outCaptor));
            FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

            // Setup MySQL source
            Configuration sourceConfig = new Configuration();
            sourceConfig.set(MySqlDataSourceOptions.HOSTNAME, MYSQL_CONTAINER.getHost());
            sourceConfig.set(MySqlDataSourceOptions.PORT, MYSQL_CONTAINER.getDatabasePort());
            sourceConfig.set(MySqlDataSourceOptions.USERNAME, TEST_USER);
            sourceConfig.set(MySqlDataSourceOptions.PASSWORD, TEST_PASSWORD);
            sourceConfig.set(MySqlDataSourceOptions.SERVER_TIME_ZONE, "UTC");
            sourceConfig.set(MySqlDataSourceOptions.TABLES, tables);
            if (tablesExclude != null) {
                sourceConfig.set(MySqlDataSourceOptions.TABLES_EXCLUDE, tablesExclude);
            }
            sourceConfig.set(
                    MySqlDataSourceOptions.SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED,
                    scanBinlogNewlyAddedTable);
            sourceConfig.set(MySqlDataSourceOptions.SERVER_ID, getServerId(1));

            SourceDef sourceDef =
                    new SourceDef(MySqlDataSourceFactory.IDENTIFIER, "MySQL Source", sourceConfig);

            // Setup value sink
            Configuration sinkConfig = new Configuration();
            sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
            SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

            // Setup pipeline
            Configuration pipelineConfig = new Configuration();
            pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
            pipelineConfig.set(
                    PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
            PipelineDef pipelineDef =
                    new PipelineDef(
                            sourceDef,
                            sinkDef,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            pipelineConfig);

            // Execute the pipeline
            PipelineExecution execution = composer.compose(pipelineDef);
            Thread executeThread =
                    new Thread(
                            () -> {
                                try {
                                    execution.execute();
                                } catch (InterruptedException ignored) {

                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            executeThread.start();

            try {
                loopCheck(
                        () -> outCaptor.toString().trim().split("\n").length >= expectedCount,
                        "collect enough rows",
                        Duration.ofSeconds(120),
                        Duration.ofSeconds(1));
            } finally {
                executeThread.interrupt();
            }
            return Arrays.asList(outCaptor.toString().trim().split("\n"));
        } finally {
            System.setOut(standardOut);
        }
    }
}
