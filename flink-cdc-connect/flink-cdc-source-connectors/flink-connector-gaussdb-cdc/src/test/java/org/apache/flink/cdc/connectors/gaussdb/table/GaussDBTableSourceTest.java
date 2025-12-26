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

package org.apache.flink.cdc.connectors.gaussdb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.IncrementalSource;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBSourceBuilder;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class GaussDBTableSourceTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.INT().notNull()),
                            Column.physical("name", DataTypes.STRING())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final String HOSTNAME = "localhost";
    private static final int PORT = 8000;
    private static final int HA_PORT = 18080;
    private static final String DATABASE = "testdb";
    private static final String SCHEMA_NAME = "public";
    private static final String TABLE_NAME = "orders";
    private static final String USERNAME = "gaussdb";
    private static final String PASSWORD = "password";
    private static final String SLOT_NAME = "gaussdb_slot";
    private static final String PLUGIN_NAME = "mppdb_decoding";
    private static final int SPLIT_SIZE = 1024;
    private static final int SPLIT_META_GROUP_SIZE = 16;
    private static final int FETCH_SIZE = 128;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
    private static final int CONNECT_MAX_RETRIES = 3;
    private static final int CONNECTION_POOL_SIZE = 2;
    private static final double DISTRIBUTION_FACTOR_UPPER = 1000.0d;
    private static final double DISTRIBUTION_FACTOR_LOWER = 0.5d;
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5);
    private static final String DBZ_HEARTBEAT_INTERVAL_MS = "1234";
    private static final String CHUNK_KEY_COLUMN = "id";
    private static final boolean CLOSE_IDLE_READERS = false;
    private static final boolean SKIP_SNAPSHOT_BACKFILL = false;
    private static final boolean SCAN_NEWLY_ADDED_TABLE_ENABLED = false;
    private static final boolean ASSIGN_UNBOUNDED_CHUNK_FIRST = false;
    private static final ScanTableSource.ScanContext SCAN_CONTEXT =
            new ScanTableSource.ScanContext() {
                @Override
                public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                    return (TypeInformation<T>) TypeInformation.of(RowData.class);
                }

                @Override
                public <T> TypeInformation<T> createTypeInformation(LogicalType producedDataType) {
                    return (TypeInformation<T>) TypeInformation.of(RowData.class);
                }

                @Override
                public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                        DataType producedDataType) {
                    return new DynamicTableSource.DataStructureConverter() {
                        @Override
                        public void open(RuntimeConverter.Context context) {}

                        @Override
                        public Object toInternal(Object externalStructure) {
                            return externalStructure;
                        }
                    };
                }
            };

    @Test
    void returnsParallelSourceWhenEnabled() {
        GaussDBTableSource tableSource = createTableSource(true);

        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(SCAN_CONTEXT);

        assertThat(provider).isInstanceOf(SourceProvider.class);

        Source<RowData, ?, ?> source = ((SourceProvider) provider).createSource();
        assertThat(source).isInstanceOf(GaussDBSourceBuilder.GaussDBIncrementalSource.class);
    }

    @Test
    void propagatesConfigurationIntoParallelSource() throws Exception {
        GaussDBTableSource tableSource = createTableSource(true);

        Source<RowData, ?, ?> source =
                ((SourceProvider) tableSource.getScanRuntimeProvider(SCAN_CONTEXT)).createSource();

        GaussDBSourceConfig config =
                extractConfig(
                        (GaussDBSourceBuilder.GaussDBIncrementalSource<RowData>) source);

        assertThat(config.getHostname()).isEqualTo(HOSTNAME);
        assertThat(config.getPort()).isEqualTo(PORT);
        assertThat(config.getDatabaseList()).containsExactly(DATABASE);
        assertThat(config.getSchemaList()).containsExactly(SCHEMA_NAME);
        assertThat(config.getTableList()).containsExactly(SCHEMA_NAME + "." + TABLE_NAME);
        assertThat(config.getSlotName()).isEqualTo(SLOT_NAME);
        assertThat(config.getDecodingPluginName()).isEqualTo(PLUGIN_NAME);
        assertThat(config.getSplitSize()).isEqualTo(SPLIT_SIZE);
        assertThat(config.getSplitMetaGroupSize()).isEqualTo(SPLIT_META_GROUP_SIZE);
        assertThat(config.getDistributionFactorUpper()).isEqualTo(DISTRIBUTION_FACTOR_UPPER);
        assertThat(config.getDistributionFactorLower()).isEqualTo(DISTRIBUTION_FACTOR_LOWER);
        assertThat(config.getFetchSize()).isEqualTo(FETCH_SIZE);
        assertThat(config.getConnectTimeout()).isEqualTo(CONNECT_TIMEOUT);
        assertThat(config.getConnectMaxRetries()).isEqualTo(CONNECT_MAX_RETRIES);
        assertThat(config.getConnectionPoolSize()).isEqualTo(CONNECTION_POOL_SIZE);
        assertThat(config.getChunkKeyColumn()).isEqualTo(CHUNK_KEY_COLUMN);
        assertThat(config.isCloseIdleReaders()).isEqualTo(CLOSE_IDLE_READERS);
        assertThat(config.isSkipSnapshotBackfill()).isEqualTo(SKIP_SNAPSHOT_BACKFILL);
        assertThat(config.isScanNewlyAddedTableEnabled())
                .isEqualTo(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        assertThat(config.isAssignUnboundedChunkFirst()).isEqualTo(ASSIGN_UNBOUNDED_CHUNK_FIRST);
        assertThat(config.getDbzConfiguration().getString("ha-port"))
                .isEqualTo(String.valueOf(HA_PORT));
        assertThat(config.getDbzConfiguration().getString("heartbeat.interval.ms"))
                .isEqualTo(DBZ_HEARTBEAT_INTERVAL_MS);
    }

    @Test
    void fallsBackToDebeziumSourceFunctionWhenParallelDisabled() throws Exception {
        GaussDBTableSource tableSource = createTableSource(false);

        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(SCAN_CONTEXT);

        assertThat(provider).isInstanceOf(SourceFunctionProvider.class);

        DebeziumSourceFunction<RowData> sourceFunction =
                (DebeziumSourceFunction<RowData>)
                        ((SourceFunctionProvider) provider).createSourceFunction();

        Properties properties = getDebeziumProperties(sourceFunction);

        assertThat(properties.getProperty("connector.class"))
                .isEqualTo("io.debezium.connector.gaussdb.GaussDBConnector");
        assertThat(properties.getProperty("plugin.name")).isEqualTo(PLUGIN_NAME);
        assertThat(properties.getProperty("database.server.name"))
                .isEqualTo("gaussdb_cdc_source");
        assertThat(properties.getProperty("database.hostname")).isEqualTo(HOSTNAME);
        assertThat(properties.getProperty("database.port")).isEqualTo(String.valueOf(PORT));
        assertThat(properties.getProperty("database.dbname")).isEqualTo(DATABASE);
        assertThat(properties.getProperty("database.user")).isEqualTo(USERNAME);
        assertThat(properties.getProperty("database.password")).isEqualTo(PASSWORD);
        assertThat(properties.getProperty("slot.name")).isEqualTo(SLOT_NAME);
        assertThat(properties.getProperty("schema.include.list")).isEqualTo(SCHEMA_NAME);
        assertThat(properties.getProperty("table.include.list"))
                .isEqualTo(SCHEMA_NAME + "." + TABLE_NAME);
        assertThat(properties.getProperty("ha-port")).isEqualTo(String.valueOf(HA_PORT));
        assertThat(properties.getProperty("heartbeat.interval.ms")).isEqualTo("1234");
        assertThat(properties.getProperty("custom.key")).isEqualTo("custom-value");
    }

    private static Properties getDebeziumProperties(DebeziumSourceFunction<?> sourceFunction)
            throws Exception {
        Field field = DebeziumSourceFunction.class.getDeclaredField("properties");
        field.setAccessible(true);
        return (Properties) field.get(sourceFunction);
    }

    private static GaussDBSourceConfig extractConfig(
            GaussDBSourceBuilder.GaussDBIncrementalSource<RowData> source) throws Exception {
        Field configFactoryField = IncrementalSource.class.getDeclaredField("configFactory");
        configFactoryField.setAccessible(true);
        GaussDBSourceConfigFactory factory =
                (GaussDBSourceConfigFactory) configFactoryField.get(source);
        return factory.create(0);
    }

    private GaussDBTableSource createTableSource(boolean enableParallelRead) {
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("heartbeat.interval.ms", DBZ_HEARTBEAT_INTERVAL_MS);
        dbzProperties.setProperty("custom.key", "custom-value");

        return new GaussDBTableSource(
                SCHEMA,
                PORT,
                HOSTNAME,
                DATABASE,
                SCHEMA_NAME,
                TABLE_NAME,
                USERNAME,
                PASSWORD,
                PLUGIN_NAME,
                SLOT_NAME,
                DebeziumChangelogMode.ALL,
                dbzProperties,
                enableParallelRead,
                SPLIT_SIZE,
                SPLIT_META_GROUP_SIZE,
                FETCH_SIZE,
                CONNECT_TIMEOUT,
                CONNECT_MAX_RETRIES,
                CONNECTION_POOL_SIZE,
                DISTRIBUTION_FACTOR_UPPER,
                DISTRIBUTION_FACTOR_LOWER,
                HEARTBEAT_INTERVAL,
                StartupOptions.initial(),
                CHUNK_KEY_COLUMN,
                CLOSE_IDLE_READERS,
                SKIP_SNAPSHOT_BACKFILL,
                SCAN_NEWLY_ADDED_TABLE_ENABLED,
                ASSIGN_UNBOUNDED_CHUNK_FIRST,
                HA_PORT);
    }
}
