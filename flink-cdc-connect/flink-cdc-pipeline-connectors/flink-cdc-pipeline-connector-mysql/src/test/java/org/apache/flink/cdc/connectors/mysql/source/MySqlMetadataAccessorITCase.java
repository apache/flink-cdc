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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlSourceTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.testutils.MySqlTypeUtils.getTestTableSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link MySqlMetadataAccessor}. */
public class MySqlMetadataAccessorITCase extends MySqlSourceTestBase {

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private final UniqueDatabase fullTypesMySql57Database =
            new UniqueDatabase(
                    MYSQL_CONTAINER,
                    "column_type_test",
                    MySqlSourceTestUtils.TEST_USER,
                    MySqlSourceTestUtils.TEST_PASSWORD);

    private final UniqueDatabase fullTypesMySql8Database =
            new UniqueDatabase(
                    MYSQL8_CONTAINER,
                    "column_type_test_mysql8",
                    MySqlSourceTestUtils.TEST_USER,
                    MySqlSourceTestUtils.TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeClass
    public static void beforeClass() {
        LOG.info("Starting MySql8 containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Container MySql8 is started.");
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("Stopping MySql8 containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Container MySql8 is stopped.");
    }

    @Before
    public void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testMysql57AccessDatabaseAndTable() {
        testAccessDatabaseAndTable(fullTypesMySql57Database);
    }

    @Test
    public void testMysql8AccessDatabaseAndTable() {
        testAccessDatabaseAndTable(fullTypesMySql8Database);
    }

    @Test
    public void testMysql57AccessCommonTypesSchemaTinyInt1isBit() {
        testAccessCommonTypesSchema(fullTypesMySql57Database, true);
    }

    @Test
    public void testMysql57AccessCommonTypesSchemaTinyInt1isNotBit() {
        testAccessCommonTypesSchema(fullTypesMySql57Database, false);
    }

    @Test
    public void testMysql8AccessCommonTypesSchemaTinyInt1isBit() {
        testAccessCommonTypesSchema(fullTypesMySql8Database, true);
    }

    @Test
    public void testMysql8AccessCommonTypesSchemaTinyInt1isNotBit() {
        testAccessCommonTypesSchema(fullTypesMySql8Database, false);
    }

    @Test
    public void testMysql57AccessTimeTypesSchema() {
        fullTypesMySql57Database.createAndInitialize();

        testAccessSchema(fullTypesMySql57Database, "time_types");
    }

    @Test
    public void testMysql8AccessTimeTypesSchema() {
        fullTypesMySql8Database.createAndInitialize();

        testAccessSchema(fullTypesMySql8Database, "time_types");
    }

    @Test
    public void testMysql57PrecisionTypesSchema() {
        fullTypesMySql57Database.createAndInitialize();

        testAccessSchema(fullTypesMySql57Database, "precision_types");
    }

    @Test
    public void testMysql8PrecisionTypesSchema() {
        fullTypesMySql8Database.createAndInitialize();

        testAccessSchema(fullTypesMySql8Database, "precision_types");
    }

    private void testAccessDatabaseAndTable(UniqueDatabase database) {
        database.createAndInitialize();

        String[] tables =
                new String[] {"common_types", "time_types", "precision_types", "json_types"};
        MySqlMetadataAccessor metadataAccessor = getMetadataAccessor(tables, database, true);

        assertThatThrownBy(metadataAccessor::listNamespaces)
                .isInstanceOf(UnsupportedOperationException.class);

        List<String> schemas = metadataAccessor.listSchemas(null);
        assertThat(schemas).contains(database.getDatabaseName());

        List<TableId> actualTables = metadataAccessor.listTables(null, database.getDatabaseName());
        List<TableId> expectedTables =
                Arrays.stream(tables)
                        .map(table -> TableId.tableId(database.getDatabaseName(), table))
                        .collect(Collectors.toList());
        assertThat(actualTables).containsExactlyInAnyOrderElementsOf(expectedTables);
    }

    private void testAccessCommonTypesSchema(UniqueDatabase database, boolean tinyint1IsBit) {
        database.createAndInitialize();

        testAccessSchema(database, "common_types", tinyint1IsBit);
    }

    private MySqlMetadataAccessor getMetadataAccessor(
            String[] tables, UniqueDatabase database, boolean tinyint1IsBit) {
        MySqlSourceConfig sourceConfig = getConfig(tables, database, tinyint1IsBit);
        return new MySqlMetadataAccessor(sourceConfig);
    }

    @Test
    public void testMysql57AccessJsonTypesSchema() {
        fullTypesMySql57Database.createAndInitialize();

        testAccessSchema(fullTypesMySql57Database, "json_types");
    }

    @Test
    public void testMysql8AccessJsonTypesSchema() {
        fullTypesMySql8Database.createAndInitialize();

        testAccessSchema(fullTypesMySql8Database, "json_types");
    }

    private void testAccessSchema(UniqueDatabase database, String tableName) {
        testAccessSchema(database, tableName, true);
    }

    private void testAccessSchema(
            UniqueDatabase database, String tableName, boolean tinyint1IsBit) {
        MySqlMetadataAccessor metadataAccessor =
                getMetadataAccessor(new String[] {tableName}, database, tinyint1IsBit);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(database.getDatabaseName(), tableName));

        Schema expectedSchema =
                getTestTableSchema(tableName, tinyint1IsBit, database == fullTypesMySql8Database);
        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    private MySqlSourceConfig getConfig(
            String[] captureTables, UniqueDatabase database, boolean tinyint1IsBit) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.latest())
                .databaseList(database.getDatabaseName())
                .tableList(captureTableIds)
                .includeSchemaChanges(false)
                .hostname(database.getHost())
                .port(database.getDatabasePort())
                .splitSize(10)
                .fetchSize(2)
                .username(database.getUsername())
                .password(database.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .treatTinyInt1AsBoolean(tinyint1IsBit)
                .createConfig(0);
    }
}
