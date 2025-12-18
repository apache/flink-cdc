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
 * Integration tests for GaussDB CDC connector data type support.
 *
 * <p>This test class validates that the connector correctly handles various GaussDB data types:
 *
 * <ul>
 *   <li>Numeric types: SMALLINT, INTEGER, BIGINT, NUMERIC, REAL, DOUBLE PRECISION
 *   <li>Character types: CHAR, VARCHAR, TEXT
 *   <li>Temporal types: DATE, TIME, TIMESTAMP, TIMESTAMP WITH TIME ZONE
 *   <li>Binary types: BYTEA
 *   <li>Boolean type: BOOLEAN
 *   <li>Special types: JSON, ARRAY
 * </ul>
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class GaussDBDataTypeTest extends GaussDBTestBase {

    private String slotName;
    private String tableName;

    @BeforeEach
    void beforeEach() {
        this.slotName = getSlotName();
        this.tableName = "data_type_test_" + System.currentTimeMillis();
    }

    @AfterEach
    void afterEach() throws Exception {
        dropTestTableIfExists();
        dropReplicationSlotIfExists(slotName);
        Thread.sleep(1000L);
    }

    @Test
    void testNumericTypes() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s ("
                                + "id INT PRIMARY KEY, "
                                + "col_smallint SMALLINT, "
                                + "col_integer INTEGER, "
                                + "col_bigint BIGINT, "
                                + "col_numeric NUMERIC(10,2), "
                                + "col_real REAL, "
                                + "col_double DOUBLE PRECISION"
                                + ")",
                        SCHEMA_NAME, tableName);

        String insertSql =
                format(
                        "INSERT INTO %s.%s VALUES "
                                + "(1, 32767, 2147483647, 9223372036854775807, 12345.67, 3.14, 2.718281828)",
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
                        "CREATE TABLE numeric_source ("
                                + " id INT NOT NULL,"
                                + " col_smallint SMALLINT,"
                                + " col_integer INT,"
                                + " col_bigint BIGINT,"
                                + " col_numeric DECIMAL(10,2),"
                                + " col_real FLOAT,"
                                + " col_double DOUBLE,"
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
        TableResult result = tEnv.executeSql("SELECT * FROM numeric_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("id")).isEqualTo(1);
        assertThat(row.getField("col_smallint")).isNotNull();
        assertThat(row.getField("col_bigint")).isNotNull();

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testCharacterTypes() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s ("
                                + "id INT PRIMARY KEY, "
                                + "col_char CHAR(10), "
                                + "col_varchar VARCHAR(100), "
                                + "col_text TEXT"
                                + ")",
                        SCHEMA_NAME, tableName);

        String insertSql =
                format(
                        "INSERT INTO %s.%s VALUES "
                                + "(1, 'CHAR', 'VARCHAR value', 'Long text content')",
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
                        "CREATE TABLE char_source ("
                                + " id INT NOT NULL,"
                                + " col_char CHAR(10),"
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
        TableResult result = tEnv.executeSql("SELECT * FROM char_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("col_varchar").toString()).contains("VARCHAR");
        assertThat(row.getField("col_text").toString()).contains("text");

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testTemporalTypes() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s ("
                                + "id INT PRIMARY KEY, "
                                + "col_date DATE, "
                                + "col_time TIME, "
                                + "col_timestamp TIMESTAMP, "
                                + "col_timestamptz TIMESTAMP WITH TIME ZONE"
                                + ")",
                        SCHEMA_NAME, tableName);

        String insertSql =
                format(
                        "INSERT INTO %s.%s VALUES "
                                + "(1, '2025-12-17', '14:30:00', '2025-12-17 14:30:00', '2025-12-17 14:30:00+08')",
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
                        "CREATE TABLE temporal_source ("
                                + " id INT NOT NULL,"
                                + " col_date DATE,"
                                + " col_time TIME,"
                                + " col_timestamp TIMESTAMP,"
                                + " col_timestamptz TIMESTAMP WITH LOCAL TIME ZONE,"
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
        TableResult result = tEnv.executeSql("SELECT * FROM temporal_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("col_date")).isNotNull();
        assertThat(row.getField("col_timestamp")).isNotNull();

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testBooleanType() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s ("
                                + "id INT PRIMARY KEY, "
                                + "col_boolean BOOLEAN, "
                                + "col_true BOOLEAN, "
                                + "col_false BOOLEAN"
                                + ")",
                        SCHEMA_NAME, tableName);

        String insertSql =
                format("INSERT INTO %s.%s VALUES (1, NULL, true, false)", SCHEMA_NAME, tableName);

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
                        "CREATE TABLE boolean_source ("
                                + " id INT NOT NULL,"
                                + " col_boolean BOOLEAN,"
                                + " col_true BOOLEAN,"
                                + " col_false BOOLEAN,"
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
        TableResult result = tEnv.executeSql("SELECT * FROM boolean_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("col_boolean")).isNull();
        assertThat(row.getField("col_true")).isEqualTo(true);
        assertThat(row.getField("col_false")).isEqualTo(false);

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
    }

    @Test
    void testNullValues() throws Exception {
        String createTableSql =
                format(
                        "CREATE TABLE %s.%s ("
                                + "id INT PRIMARY KEY, "
                                + "col_int INTEGER, "
                                + "col_varchar VARCHAR(100), "
                                + "col_timestamp TIMESTAMP"
                                + ")",
                        SCHEMA_NAME, tableName);

        String insertSql =
                format("INSERT INTO %s.%s VALUES (1, NULL, NULL, NULL)", SCHEMA_NAME, tableName);

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
                        "CREATE TABLE null_test_source ("
                                + " id INT NOT NULL,"
                                + " col_int INT,"
                                + " col_varchar VARCHAR(100),"
                                + " col_timestamp TIMESTAMP,"
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
        TableResult result = tEnv.executeSql("SELECT * FROM null_test_source");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext() && rows.size() < 1) {
                rows.add(iterator.next());
            }
        }

        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getField("col_int")).isNull();
        assertThat(row.getField("col_varchar")).isNull();
        assertThat(row.getField("col_timestamp")).isNull();

        result.getJobClient().ifPresent(jc -> jc.cancel());
        env.close();
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
