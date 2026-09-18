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

package org.apache.flink.cdc.connectors.db2.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.connectors.db2.source.DatabaseNameMetadataColumn;
import org.apache.flink.cdc.connectors.db2.source.Db2DataSource;
import org.apache.flink.cdc.connectors.db2.source.OpTsMetadataColumn;
import org.apache.flink.cdc.connectors.db2.source.SchemaNameMetadataColumn;
import org.apache.flink.cdc.connectors.db2.source.TableNameMetadataColumn;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Db2DataSourceFactory}. */
class Db2DataSourceFactoryTest {

    @Test
    void testIdentifierAndOptions() {
        Db2DataSourceFactory factory = new Db2DataSourceFactory();

        assertThat(factory.identifier()).isEqualTo("db2");
        assertThat(
                        factory.requiredOptions().stream()
                                .map(ConfigOption::key)
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(
                        HOSTNAME.key(), USERNAME.key(), PASSWORD.key(), TABLES.key());
        assertThat(factory.optionalOptions()).contains(PORT, TABLES_EXCLUDE, METADATA_LIST);
        assertThat(factory.optionalOptions().stream().map(ConfigOption::key))
                .contains(SCAN_STARTUP_MODE.key())
                .doesNotContain("scan.startup.timestamp-millis")
                .doesNotContain("scan.newly-added-table.enabled");
    }

    @Test
    void testLackRequiredOptions() {
        Db2DataSourceFactory factory = new Db2DataSourceFactory();

        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> options = validOptions();
            options.remove(requiredOption.key());

            assertThatThrownBy(
                            () ->
                                    factory.createDataSource(
                                            new MockContext(Configuration.fromMap(options))))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(requiredOption.key());
        }
    }

    @Test
    void testUnsupportedOption() {
        Map<String, String> options = validOptions();
        options.put("unsupported_key", "unsupported_value");

        assertThatThrownBy(
                        () ->
                                new Db2DataSourceFactory()
                                        .createDataSource(
                                                new MockContext(Configuration.fromMap(options))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'db2'")
                .hasMessageContaining("unsupported_key");
    }

    @Test
    void testUnsupportedStartupModesFailBeforeDatabaseAccess() {
        assertUnsupportedStartupMode("snapshot");
        assertUnsupportedStartupMode("timestamp");
    }

    @Test
    void testTableValidationWithDifferentDatabases() {
        Map<String, String> options = validOptions();
        options.put(TABLES.key(), "DB1.DB2INST1.T1,DB2.DB2INST1.T2");

        assertThatThrownBy(
                        () ->
                                new Db2DataSourceFactory()
                                        .createDataSource(
                                                new MockContext(Configuration.fromMap(options))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not all table names have the same database name");
    }

    @Test
    void testTableValidationRequiresDatabaseSchemaTableFormat() {
        Map<String, String> options = validOptions();
        options.put(TABLES.key(), "TESTDB.DB2INST1");

        assertThatThrownBy(
                        () ->
                                new Db2DataSourceFactory()
                                        .createDataSource(
                                                new MockContext(Configuration.fromMap(options))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not match the expected 'database.schema.table' format")
                .hasMessageContaining(TABLES.key());
    }

    @Test
    void testDatabaseNameLengthValidation() {
        Map<String, String> options = validOptions();
        options.put(TABLES.key(), repeat("D", 129) + ".DB2INST1.T1");

        assertThatThrownBy(
                        () ->
                                new Db2DataSourceFactory()
                                        .createDataSource(
                                                new MockContext(Configuration.fromMap(options))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exceeds DB2's maximum identifier length");
    }

    @Test
    void testSupportedMetadataColumns() {
        Db2DataSource dataSource = new Db2DataSource(testConfigFactory());

        SupportedMetadataColumn[] metadataColumns = dataSource.supportedMetadataColumns();

        assertThat(metadataColumns).hasSize(4);
        assertThat(metadataColumns[0]).isInstanceOf(OpTsMetadataColumn.class);
        assertThat(metadataColumns[0].getName()).isEqualTo("op_ts");
        assertThat(metadataColumns[1]).isInstanceOf(TableNameMetadataColumn.class);
        assertThat(metadataColumns[1].getName()).isEqualTo("table_name");
        assertThat(metadataColumns[2]).isInstanceOf(DatabaseNameMetadataColumn.class);
        assertThat(metadataColumns[2].getName()).isEqualTo("database_name");
        assertThat(metadataColumns[3]).isInstanceOf(SchemaNameMetadataColumn.class);
        assertThat(metadataColumns[3].getName()).isEqualTo("schema_name");

        Map<String, String> metadata = new HashMap<>();
        metadata.put("op_ts", "12345");
        metadata.put("table_name", "PRODUCTS");
        metadata.put("database_name", "TESTDB");
        metadata.put("schema_name", "DB2INST1");
        assertThat(metadataColumns[0].read(metadata)).isEqualTo(12345L);
        assertThat(metadataColumns[1].read(metadata)).isEqualTo("PRODUCTS");
        assertThat(metadataColumns[2].read(metadata)).isEqualTo("TESTDB");
        assertThat(metadataColumns[3].read(metadata)).isEqualTo("DB2INST1");
    }

    private static void assertUnsupportedStartupMode(String startupMode) {
        Map<String, String> options = validOptions();
        options.put(SCAN_STARTUP_MODE.key(), startupMode);

        assertThatThrownBy(
                        () ->
                                new Db2DataSourceFactory()
                                        .createDataSource(
                                                new MockContext(Configuration.fromMap(options))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Supported values are [initial, latest-offset]")
                .hasMessageContaining(startupMode);
    }

    private static Map<String, String> validOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), "localhost");
        options.put(USERNAME.key(), "db2inst1");
        options.put(PASSWORD.key(), "password");
        options.put(TABLES.key(), "TESTDB.DB2INST1.PRODUCTS");
        return options;
    }

    private static Db2SourceConfigFactory testConfigFactory() {
        Db2SourceConfigFactory configFactory = new Db2SourceConfigFactory();
        Properties dbzProperties = new Properties();
        configFactory
                .hostname("localhost")
                .port(50000)
                .databaseList("TESTDB")
                .tableList("DB2INST1.PRODUCTS")
                .username("db2inst1")
                .password("password")
                .serverTimeZone("UTC")
                .debeziumProperties(dbzProperties);
        return configFactory;
    }

    private static String repeat(String value, int times) {
        StringBuilder builder = new StringBuilder(value.length() * times);
        for (int i = 0; i < times; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static class MockContext implements Factory.Context {

        private final Configuration factoryConfiguration;

        private MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return Configuration.fromMap(Collections.emptyMap());
        }

        @Override
        public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
        }
    }
}
