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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.db2.source.Db2DataSource;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.db2.source.Db2DataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.Db2Container.DB2_PORT;

/** Testcontainers-backed tests for {@link Db2DataSourceFactory}. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class Db2DataSourceFactoryContainerTest extends PipelineDb2TestBase {

    private static final String DATABASE_NAME = "TESTDB";
    private static final String SCHEMA_NAME = "DB2INST1";

    @BeforeEach
    void before() {
        initializeDb2Table("inventory", "PRODUCTS");
        initializeDb2Table("customers", "CUSTOMERS");
    }

    @Test
    void testCreateDataSourceWithExactTable() {
        Map<String, String> options = containerOptions(DATABASE_NAME + ".DB2INST1.PRODUCTS");

        Db2DataSource dataSource = createDataSource(options);

        assertThat(dataSource.getDb2SourceConfig().getTableList())
                .containsExactly("DB2INST1.PRODUCTS");
    }

    @Test
    void testCreateDataSourceWithWildcardAndExclude() {
        Map<String, String> options = containerOptions(DATABASE_NAME + ".DB2INST1.\\.*");
        options.put(TABLES_EXCLUDE.key(), DATABASE_NAME + ".DB2INST1.CUSTOMERS");

        Db2DataSource dataSource = createDataSource(options);

        assertThat(dataSource.getDb2SourceConfig().getTableList())
                .contains("DB2INST1.PRODUCTS")
                .doesNotContain("DB2INST1.CUSTOMERS");
    }

    @Test
    void testExcludeAllMatchedTables() {
        Map<String, String> options = containerOptions(DATABASE_NAME + ".DB2INST1.PRODUCTS");
        options.put(TABLES_EXCLUDE.key(), DATABASE_NAME + ".DB2INST1.PRODUCTS");

        assertThatThrownBy(() -> createDataSource(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table with the option 'tables.exclude'");
    }

    @Test
    void testChunkKeyColumnOptionIsForwarded() {
        Map<String, String> options = containerOptions(DATABASE_NAME + ".DB2INST1.PRODUCTS");
        options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), "ID");

        Db2DataSource dataSource = createDataSource(options);

        assertThat(dataSource.getDb2SourceConfig().getChunkKeyColumn()).isEqualTo("ID");
    }

    @Test
    void testMetadataAccessorListsDb2ObjectsAndSchema() {
        Db2DataSource dataSource =
                createDataSource(containerOptions(DATABASE_NAME + ".DB2INST1.PRODUCTS"));
        MetadataAccessor metadataAccessor = dataSource.getMetadataAccessor();

        assertThat(metadataAccessor.listNamespaces()).containsExactly(DATABASE_NAME);
        assertThat(metadataAccessor.listSchemas(DATABASE_NAME)).contains(SCHEMA_NAME);
        assertThat(metadataAccessor.listTables(DATABASE_NAME, SCHEMA_NAME))
                .contains(TableId.tableId(DATABASE_NAME, SCHEMA_NAME, "PRODUCTS"));

        Schema schema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(DATABASE_NAME, SCHEMA_NAME, "PRODUCTS"));
        assertThat(schema.getColumnNames()).containsExactly("ID", "NAME", "DESCRIPTION", "WEIGHT");
        assertThat(schema.primaryKeys()).containsExactly("ID");
    }

    @Test
    void testPipelineSourceReadsSnapshotEvents() throws Exception {
        Map<String, String> options = containerOptions(DATABASE_NAME + ".DB2INST1.PRODUCTS");
        options.put(METADATA_LIST.key(), "database_name,schema_name,table_name,op_ts");
        Db2DataSource dataSource = createDataSource(options);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());

        try (CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                Db2DataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect()) {
            TableId tableId = TableId.tableId(DATABASE_NAME, SCHEMA_NAME, "PRODUCTS");
            List<DataChangeEvent> snapshotEvents = fetchSnapshotEvents(events, 9);

            assertThat(snapshotEvents)
                    .allSatisfy(
                            event -> {
                                assertThat(event.tableId()).isEqualTo(tableId);
                                assertThat(event.op()).isEqualTo(OperationType.INSERT);
                                assertThat(event.after()).isNotNull();
                                assertThat(event.meta())
                                        .containsEntry("database_name", DATABASE_NAME)
                                        .containsEntry("schema_name", SCHEMA_NAME)
                                        .containsEntry("table_name", "PRODUCTS")
                                        .containsKey("op_ts");
                            });
        }
    }

    private static List<DataChangeEvent> fetchSnapshotEvents(Iterator<Event> events, int size) {
        List<CreateTableEvent> createTableEvents = new ArrayList<>();
        List<DataChangeEvent> dataChangeEvents = new ArrayList<>();
        while (events.hasNext()) {
            Event event = events.next();
            if (event instanceof CreateTableEvent) {
                createTableEvents.add((CreateTableEvent) event);
            } else if (event instanceof DataChangeEvent) {
                dataChangeEvents.add((DataChangeEvent) event);
                if (dataChangeEvents.size() == size) {
                    break;
                }
            }
        }
        assertThat(createTableEvents).isNotEmpty();
        return dataChangeEvents;
    }

    private static Db2DataSource createDataSource(Map<String, String> options) {
        return (Db2DataSource)
                new Db2DataSourceFactory()
                        .createDataSource(new MockContext(Configuration.fromMap(options)));
    }

    private static Map<String, String> containerOptions(String tables) {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), DB2_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(DB2_CONTAINER.getMappedPort(DB2_PORT)));
        options.put(USERNAME.key(), DB2_CONTAINER.getUsername());
        options.put(PASSWORD.key(), DB2_CONTAINER.getPassword());
        options.put(TABLES.key(), tables);
        return options;
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
