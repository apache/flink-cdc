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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.RuntimeExecutionMode;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.factory.OracleDataSourceFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link OracleDataSourceFactory}. */
public class OracleDataSourceFactoryTest extends OracleSourceTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        LOG.info("Starting oracle19c containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Container oracle19c is started.");
    }

    @BeforeEach
    public void resetTables() throws Exception {
        createAndInitialize("product.sql");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping oracle19c containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Container oracle19c is stopped.");
    }

    @Test
    public void testCreateSource() {
        OracleDataSource dataSource =
                createDataSource(baseOptions("debezium.products,debezium.category"));

        assertThat(dataSource.getSourceConfig().getTableList())
                .containsExactlyInAnyOrder("debezium.products", "debezium.category");
    }

    @Test
    public void testNoMatchedTable() {
        Map<String, String> options = baseOptions("DEBEZIUM.TEST");

        assertThatThrownBy(() -> createDataSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any table by the option 'tables' = "
                                + options.get(TABLES.key()));
    }

    @Test
    public void testExcludeTable() {
        Map<String, String> options = baseOptions("debezium.products,debezium.category");
        options.put(TABLES_EXCLUDE.key(), "debezium.category");

        OracleDataSource dataSource = createDataSource(options);

        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("debezium.products"));
    }

    @Test
    public void testExcludeAllTable() {
        Map<String, String> options = baseOptions("debezium.products");
        options.put(TABLES_EXCLUDE.key(), "debezium.products");

        assertThatThrownBy(() -> createDataSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any table after applying 'tables.exclude' = debezium.products");
    }

    @Test
    public void testSchemaChangeEnabledDefault() {
        OracleDataSource dataSource = createDataSource(baseOptions("debezium.products"));

        assertThat(dataSource.getSourceConfig().isIncludeSchemaChanges()).isTrue();
    }

    @Test
    public void testSchemaChangeDisabled() {
        Map<String, String> options = baseOptions("debezium.products");
        options.put(SCHEMA_CHANGE_ENABLED.key(), "false");

        OracleDataSource dataSource = createDataSource(options);

        assertThat(dataSource.getSourceConfig().isIncludeSchemaChanges()).isFalse();
    }

    @Test
    public void testChunkKeyColumnMapping() {
        Map<String, String> options = baseOptions("debezium.products,debezium.category");
        options.put(
                SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(),
                "debezium.products:ID;debezium.category:ID;");

        OracleDataSource dataSource = createDataSource(options);

        assertThat(dataSource.getSourceConfig().getChunkKeyColumn())
                .isEqualTo("debezium.products:ID;debezium.category:ID");
    }

    @Test
    public void testChunkKeyColumnMappingForTableWithoutPrimaryKey() throws Exception {
        createAndInitialize("chunk_key_no_pk.sql");

        Map<String, String> options = baseOptions("debezium.chunk_key_no_pk");
        options.put(
                SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), "debezium.chunk_key_no_pk:ID");

        OracleDataSource dataSource = createDataSource(options);

        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("debezium.chunk_key_no_pk"));
        assertThat(dataSource.getSourceConfig().getChunkKeyColumn())
                .isEqualTo("debezium.chunk_key_no_pk:ID");
    }

    @Test
    public void testChunkKeyColumnInvalidFormat() {
        Map<String, String> options = baseOptions("debezium.products");
        options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), "debezium.products:ID:EXTRA");

        assertThatThrownBy(() -> createDataSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "failed to be parsed in this part 'debezium.products:ID:EXTRA'");
    }

    @Test
    public void testBatchModeWithSnapshotStartup() {
        Map<String, String> options = baseOptions("debezium.products");
        options.put(SCAN_STARTUP_MODE.key(), "snapshot");

        OracleDataSource dataSource =
                createDataSource(
                        new MockContext(
                                Configuration.fromMap(options),
                                createBatchPipelineConfiguration()));

        assertThat(dataSource.getSourceConfig().getStartupOptions())
                .isEqualTo(StartupOptions.snapshot());
    }

    @Test
    public void testBatchModeWithInitialStartup() {
        Map<String, String> options = baseOptions("debezium.products");
        options.put(SCAN_STARTUP_MODE.key(), "initial");

        assertThatThrownBy(
                        () ->
                                createDataSource(
                                        new MockContext(
                                                Configuration.fromMap(options),
                                                createBatchPipelineConfiguration())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Only \"snapshot\" of OracleDataSource StartupOption is supported in BATCH pipeline");
    }

    @Test
    public void testBatchModeWithLatestStartup() {
        Map<String, String> options = baseOptions("debezium.products");
        options.put(SCAN_STARTUP_MODE.key(), "latest-offset");

        assertThatThrownBy(
                        () ->
                                createDataSource(
                                        new MockContext(
                                                Configuration.fromMap(options),
                                                createBatchPipelineConfiguration())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Only \"snapshot\" of OracleDataSource StartupOption is supported in BATCH pipeline");
    }

    @Test
    public void testOptionalOption() {
        Map<String, String> options = baseOptions("debezium.products");
        options.put(TABLES_EXCLUDE.key(), "debezium.category");
        options.put(SCHEMA_CHANGE_ENABLED.key(), "false");
        options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), "debezium.products:ID");

        OracleDataSourceFactory factory = new OracleDataSourceFactory();
        assertThat(factory.optionalOptions())
                .contains(
                        TABLES_EXCLUDE,
                        SCHEMA_CHANGE_ENABLED,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);

        OracleDataSource dataSource =
                (OracleDataSource)
                        factory.createDataSource(new MockContext(Configuration.fromMap(options)));
        assertThat(dataSource.getSourceConfig().isIncludeSchemaChanges()).isFalse();
        assertThat(dataSource.getSourceConfig().getChunkKeyColumn())
                .isEqualTo("debezium.products:ID");
    }

    private static OracleDataSource createDataSource(Map<String, String> options) {
        return createDataSource(new MockContext(Configuration.fromMap(options)));
    }

    private static OracleDataSource createDataSource(Factory.Context context) {
        OracleDataSourceFactory factory = new OracleDataSourceFactory();
        return (OracleDataSource) factory.createDataSource(context);
    }

    private static Map<String, String> baseOptions(String tables) {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(TABLES.key(), tables);
        return options;
    }

    private static Configuration createBatchPipelineConfiguration() {
        Configuration pipelineConfiguration = new Configuration();
        pipelineConfiguration.set(
                PipelineOptions.PIPELINE_EXECUTION_RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        return pipelineConfiguration;
    }

    static class MockContext implements Factory.Context {

        private final Configuration factoryConfiguration;
        private final Configuration pipelineConfiguration;

        MockContext(Configuration factoryConfiguration) {
            this(factoryConfiguration, null);
        }

        MockContext(Configuration factoryConfiguration, Configuration pipelineConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
            this.pipelineConfiguration = pipelineConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return pipelineConfiguration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClass().getClassLoader();
        }
    }
}
