/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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
import org.apache.flink.cdc.connectors.db2.Db2TestBase;
import org.apache.flink.cdc.connectors.db2.source.Db2DataSource;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Db2Container;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Db2DataSourceFactory}. */
public class Db2DataSourceFactoryTest extends Db2TestBase {

    private static final String SCHEMA_NAME = "DB2INST1";

    private static final String TABLE_NAME = "CUSTOMERS";

    @BeforeEach
    public void before() {
        initializeDb2Table("customers", TABLE_NAME);
    }

    private Map<String, String> getBaseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), DB2_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(DB2_CONTAINER.getMappedPort(Db2Container.DB2_PORT)));
        options.put(USERNAME.key(), DB2_CONTAINER.getUsername());
        options.put(PASSWORD.key(), DB2_CONTAINER.getPassword());
        options.put(DATABASE.key(), DB2_CONTAINER.getDatabaseName());
        options.put(TABLES.key(), SCHEMA_NAME + "." + TABLE_NAME);
        return options;
    }

    @Test
    public void testCreateDataSource() {
        Factory.Context context = new MockContext(Configuration.fromMap(getBaseOptions()));
        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        Db2DataSource dataSource = (Db2DataSource) factory.createDataSource(context);
        assertThat(dataSource.getDb2SourceConfig().getTableList())
                .containsExactly(SCHEMA_NAME + "." + TABLE_NAME);
    }

    @Test
    public void testNoMatchedTable() {
        Map<String, String> options = getBaseOptions();
        String tables = SCHEMA_NAME + ".nonexistent";
        options.put(TABLES.key(), tables);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table by the option 'tables' = " + tables);
    }

    @Test
    public void testExcludeAllTable() {
        Map<String, String> options = getBaseOptions();
        String tablesExclude = SCHEMA_NAME + "." + TABLE_NAME;
        options.put(TABLES_EXCLUDE.key(), tablesExclude);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any table with by the option 'tables.exclude' = "
                                + tablesExclude);
    }

    @Test
    public void testLackRequireOption() {
        Map<String, String> options = getBaseOptions();

        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        List<String> requireKeys =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        for (String requireKey : requireKeys) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Factory.Context context = new MockContext(Configuration.fromMap(remainingOptions));

            assertThatThrownBy(() -> factory.createDataSource(context))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    public void testUnsupportedOption() {
        Map<String, String> options = getBaseOptions();
        options.put("unsupported_key", "unsupported_value");

        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'db2'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testOptionalOption() {
        Map<String, String> options = getBaseOptions();

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        assertThat(factory.optionalOptions()).contains(PORT);

        Db2DataSource dataSource = (Db2DataSource) factory.createDataSource(context);
        assertThat(dataSource.getDb2SourceConfig().getPort())
                .isEqualTo(DB2_CONTAINER.getMappedPort(Db2Container.DB2_PORT));
    }

    @Test
    public void testChunkKeyColumnOptionIsSupported() {
        Map<String, String> options = getBaseOptions();
        options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), "ID");

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        Db2DataSourceFactory factory = new Db2DataSourceFactory();

        assertThat(factory.optionalOptions()).contains(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        Db2DataSource dataSource = (Db2DataSource) factory.createDataSource(context);
        assertThat(dataSource.getDb2SourceConfig().getChunkKeyColumn()).isEqualTo("ID");
    }

    @Test
    public void testUnsupportedStartupMode() {
        Map<String, String> options = getBaseOptions();
        options.put(SCAN_STARTUP_MODE.key(), "timestamp");

        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid value for option 'scan.startup.mode'");
    }

    @Test
    public void testLatestOffsetStartupMode() {
        Map<String, String> options = getBaseOptions();
        options.put(SCAN_STARTUP_MODE.key(), "latest-offset");

        Factory.Context context = new MockContext(Configuration.fromMap(options));
        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        Db2DataSource dataSource = (Db2DataSource) factory.createDataSource(context);
        assertThat(dataSource.getDb2SourceConfig().getStartupOptions().isStreamOnly()).isTrue();
    }

    @Test
    public void testPrefixRequireOption() {
        Map<String, String> options = getBaseOptions();
        options.put("debezium.snapshot.mode", "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        Db2DataSource dataSource = (Db2DataSource) factory.createDataSource(context);
        assertThat(dataSource.getDb2SourceConfig().getTableList())
                .containsExactly(SCHEMA_NAME + "." + TABLE_NAME);
    }

    @Test
    public void testInvalidMetadataList() {
        Map<String, String> options = getBaseOptions();
        options.put(METADATA_LIST.key(), "database_name,unknown_metadata");

        Db2DataSourceFactory factory = new Db2DataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be found in Db2 metadata");
    }

    static class MockContext implements Factory.Context {

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
            return this.getClass().getClassLoader();
        }
    }
}
