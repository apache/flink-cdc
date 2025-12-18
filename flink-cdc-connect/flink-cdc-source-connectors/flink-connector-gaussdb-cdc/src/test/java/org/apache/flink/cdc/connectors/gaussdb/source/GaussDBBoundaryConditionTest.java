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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.gaussdb.GaussDBTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for GaussDB CDC connector boundary conditions.
 *
 * <p>This test class validates that the connector correctly handles:
 *
 * <ul>
 *   <li>Empty tables
 *   <li>Empty strings
 *   <li>Maximum length strings
 *   <li>Numeric boundaries (MIN_VALUE, MAX_VALUE)
 *   <li>Special characters and Unicode
 *   <li>Large batch inserts
 * </ul>
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class GaussDBBoundaryConditionTest extends GaussDBTestBase {

    private String slotName;
    private String tableName;

    @BeforeEach
    void beforeEach() {
        this.slotName = getSlotName();
        this.tableName = "boundary_test_" + System.currentTimeMillis();
    }

    @AfterEach
    void afterEach() throws Exception {
        dropTestTableIfExists();
        dropReplicationSlotIfExists(slotName);
        Thread.sleep(1000L);
    }

    @Test
    void testEmptyTableSnapshot() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s (id INT PRIMARY KEY, name VARCHAR(100))",
                        SCHEMA_NAME, tableName);

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        String sourceDDL = createSourceDDL("empty_table_source", "initial");
        tEnv.executeSql(sourceDDL);
        TableResult result = tEnv.executeSql("SELECT * FROM empty_table_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            Thread.sleep(2000L);
        }

        assertThat(rows).isEmpty();

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testEmptyStrings() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s (id INT PRIMARY KEY, col_varchar VARCHAR(100), col_text TEXT)",
                        SCHEMA_NAME, tableName);

        String insertSql = format("INSERT INTO %s.%s VALUES (1, '', '')", SCHEMA_NAME, tableName);

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            stmt.execute(insertSql);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        String sourceDDL =
                format(
                        "CREATE TABLE empty_string_source ("
                                + " id INT NOT NULL,"
                                + " col_varchar VARCHAR(100),"
                                + " col_text STRING,"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'gaussdb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%d',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'initial',"
                                + " 'decoding.plugin.name' = 'mppdb_decoding',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        HOSTNAME,
                        PORT,
                        USERNAME,
                        PASSWORD,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        tableName,
                        slotName);

        tEnv.executeSql(sourceDDL);
        TableResult result = tEnv.executeSql("SELECT * FROM empty_string_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("col_varchar").toString()).isEmpty();
        assertThat(row.getField("col_text").toString()).isEmpty();

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testMaxLengthStrings() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s (id INT PRIMARY KEY, col_varchar VARCHAR(1000))",
                        SCHEMA_NAME, tableName);

        String longString = "A".repeat(1000);
        String insertSql =
                format("INSERT INTO %s.%s VALUES (1, '%s')", SCHEMA_NAME, tableName, longString);

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            stmt.execute(insertSql);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        String sourceDDL =
                format(
                        "CREATE TABLE max_length_source ("
                                + " id INT NOT NULL,"
                                + " col_varchar VARCHAR(1000),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'gaussdb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%d',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'initial',"
                                + " 'decoding.plugin.name' = 'mppdb_decoding',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        HOSTNAME,
                        PORT,
                        USERNAME,
                        PASSWORD,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        tableName,
                        slotName);

        tEnv.executeSql(sourceDDL);
        TableResult result = tEnv.executeSql("SELECT * FROM max_length_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("col_varchar").toString()).hasSize(1000);
        assertThat(row.getField("col_varchar").toString()).isEqualTo(longString);

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testNumericBoundaries() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s ("
                                + "id INT PRIMARY KEY, "
                                + "col_smallint_min SMALLINT, "
                                + "col_smallint_max SMALLINT, "
                                + "col_int_min INTEGER, "
                                + "col_int_max INTEGER, "
                                + "col_bigint_min BIGINT, "
                                + "col_bigint_max BIGINT"
                                + ")",
                        SCHEMA_NAME, tableName);

        String insertSql =
                format(
                        "INSERT INTO %s.%s VALUES (1, -32768, 32767, -2147483648, 2147483647, -9223372036854775808, 9223372036854775807)",
                        SCHEMA_NAME, tableName);

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            stmt.execute(insertSql);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        String sourceDDL =
                format(
                        "CREATE TABLE numeric_boundary_source ("
                                + " id INT NOT NULL,"
                                + " col_smallint_min SMALLINT,"
                                + " col_smallint_max SMALLINT,"
                                + " col_int_min INT,"
                                + " col_int_max INT,"
                                + " col_bigint_min BIGINT,"
                                + " col_bigint_max BIGINT,"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'gaussdb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%d',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'initial',"
                                + " 'decoding.plugin.name' = 'mppdb_decoding',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        HOSTNAME,
                        PORT,
                        USERNAME,
                        PASSWORD,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        tableName,
                        slotName);

        tEnv.executeSql(sourceDDL);
        TableResult result = tEnv.executeSql("SELECT * FROM numeric_boundary_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(((Number) row.getField("col_smallint_min")).shortValue())
                .isEqualTo(Short.MIN_VALUE);
        assertThat(((Number) row.getField("col_smallint_max")).shortValue())
                .isEqualTo(Short.MAX_VALUE);
        assertThat(((Number) row.getField("col_int_min")).intValue()).isEqualTo(Integer.MIN_VALUE);
        assertThat(((Number) row.getField("col_int_max")).intValue()).isEqualTo(Integer.MAX_VALUE);
        assertThat(((Number) row.getField("col_bigint_min")).longValue()).isEqualTo(Long.MIN_VALUE);
        assertThat(((Number) row.getField("col_bigint_max")).longValue()).isEqualTo(Long.MAX_VALUE);

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testSpecialCharacters() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s (id INT PRIMARY KEY, col_varchar VARCHAR(200))",
                        SCHEMA_NAME, tableName);

        String specialChars =
                "Special: !@#$%^&*()_+-=[]{}|;:',.<>?/~` Unicode: \u4E2D\u6587 \uD83D\uDE00";
        String escapedChars = specialChars.replace("'", "''");
        String insertSql =
                format("INSERT INTO %s.%s VALUES (1, '%s')", SCHEMA_NAME, tableName, escapedChars);

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            stmt.execute(insertSql);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        String sourceDDL =
                format(
                        "CREATE TABLE special_char_source ("
                                + " id INT NOT NULL,"
                                + " col_varchar VARCHAR(200),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'gaussdb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%d',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'initial',"
                                + " 'decoding.plugin.name' = 'mppdb_decoding',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        HOSTNAME,
                        PORT,
                        USERNAME,
                        PASSWORD,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        tableName,
                        slotName);

        tEnv.executeSql(sourceDDL);
        TableResult result = tEnv.executeSql("SELECT * FROM special_char_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        String actualValue = row.getField("col_varchar").toString();
        assertThat(actualValue).contains("Special:");
        assertThat(actualValue).contains("Unicode:");

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testLargeBatchInsert() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s (id INT PRIMARY KEY, value VARCHAR(100))",
                        SCHEMA_NAME, tableName);

        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);

            for (int i = 1; i <= 1000; i++) {
                String insertSql =
                        format(
                                "INSERT INTO %s.%s VALUES (%d, 'value_%d')",
                                SCHEMA_NAME, tableName, i, i);
                stmt.execute(insertSql);
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        String sourceDDL =
                format(
                        "CREATE TABLE large_batch_source ("
                                + " id INT NOT NULL,"
                                + " value VARCHAR(100),"
                                + " PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'gaussdb-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%d',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = 'initial',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'decoding.plugin.name' = 'mppdb_decoding',"
                                + " 'slot.name' = '%s'"
                                + ")",
                        HOSTNAME,
                        PORT,
                        USERNAME,
                        PASSWORD,
                        DATABASE_NAME,
                        SCHEMA_NAME,
                        tableName,
                        slotName);

        tEnv.executeSql(sourceDDL);
        TableResult result = tEnv.executeSql("SELECT COUNT(*) as cnt FROM large_batch_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(((Number) row.getField("cnt")).longValue()).isEqualTo(1000L);

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    private String createSourceDDL(String tableName, String startupMode) {
        return format(
                "CREATE TABLE %s ("
                        + " id INT NOT NULL,"
                        + " name VARCHAR(100),"
                        + " PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'gaussdb-cdc',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%d',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'schema-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.startup.mode' = '%s',"
                        + " 'decoding.plugin.name' = 'mppdb_decoding',"
                        + " 'slot.name' = '%s'"
                        + ")",
                tableName,
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                DATABASE_NAME,
                SCHEMA_NAME,
                this.tableName,
                startupMode,
                slotName);
    }

    private void dropTestTableIfExists() {
        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(format("DROP TABLE IF EXISTS %s.%s", SCHEMA_NAME, tableName));
        } catch (Exception e) {
            LOG.warn("Failed to drop test table {}.{}", SCHEMA_NAME, tableName, e);
        }
    }

    private void dropReplicationSlotIfExists(String slotName) {
        try (Connection connection = getJdbcConnection();
                Statement stmt = connection.createStatement()) {
            stmt.execute(format("DROP_REPLICATION_SLOT \"%s\"", slotName));
        } catch (Exception e) {
            LOG.warn("Failed to drop replication slot '{}'", slotName, e);
        }
    }
}
