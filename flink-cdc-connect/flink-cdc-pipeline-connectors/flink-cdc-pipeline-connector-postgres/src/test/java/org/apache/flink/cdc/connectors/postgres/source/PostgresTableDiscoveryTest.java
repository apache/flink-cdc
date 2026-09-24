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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresTableDiscoveryTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void filtersCapturedTablesAndRespectsDatabaseAndPartitionOptions(
            boolean includeDatabase, boolean includePartitions) {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.tables.addAll(
                Arrays.asList(
                        table("sales", "orders_1"),
                        table("sales", "orders$2026"),
                        table("sales", "orders_archive"),
                        table("sales", "customers"),
                        table("other", "orders_1")));
        PostgresDataSource source = source(jdbc, includeDatabase, includePartitions);

        assertThat(source.listCapturedTables())
                .containsExactlyInAnyOrder(
                        includeDatabase
                                ? TableId.tableId("inventory", "sales", "orders_1")
                                : TableId.tableId("sales", "orders_1"),
                        includeDatabase
                                ? TableId.tableId("inventory", "sales", "orders$2026")
                                : TableId.tableId("sales", "orders$2026"));
        assertThat(jdbc.requestedTypes)
                .containsExactlyElementsOf(
                        includePartitions
                                ? Arrays.asList("TABLE", "PARTITIONED TABLE")
                                : Collections.singletonList("TABLE"));
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void returnsEmptyWhenMetadataIsEmptyOrAllTablesAreFilteredOut(boolean hasTables) {
        MetadataConnection jdbc = new MetadataConnection();
        if (hasTables) {
            jdbc.tables.addAll(
                    Arrays.asList(table("sales", "customers"), table("sales", "orders_archive")));
        }
        assertThat(source(jdbc, false, false).listCapturedTables()).isEmpty();
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void propagatesRuntimeMetadataFailuresAndClosesTheConnection() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.failure = new IllegalStateException("metadata unavailable");
        PostgresDataSource source = source(jdbc, false, false);

        assertThatThrownBy(source::listCapturedTables).isSameAs(jdbc.failure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void wrapsMetadataSqlExceptionAndClosesTheConnection() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.metadataFailure = new SQLException("table metadata unavailable");
        PostgresDataSource source = source(jdbc, false, false);

        assertThatThrownBy(source::listCapturedTables)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasCause(jdbc.metadataFailure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void wrapsConnectionCloseSqlException() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.closeFailure = new SQLException("connection close failed");
        PostgresDataSource source = source(jdbc, false, false);

        assertThatThrownBy(source::listCapturedTables)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasCause(jdbc.closeFailure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    private static PostgresDataSource source(
            MetadataConnection jdbc, boolean includeDatabase, boolean includePartitions) {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("localhost")
                .port(5432)
                .username("test")
                .password("test")
                .tableList("sales\\.orders_1", "sales\\.orders\\$2026");
        factory.database("inventory");
        factory.schemaList(new String[] {"sales"});
        factory.setIncludeDatabaseInTableId(includeDatabase);
        factory.setIncludePartitionedTables(includePartitions);
        return new PostgresDataSource(factory) {
            @Override
            PostgresDialect createTableDiscoveryDialect() {
                return new PostgresDialect(getPostgresSourceConfig()) {
                    @Override
                    public JdbcConnection openJdbcConnection(JdbcSourceConfig config) {
                        assertThat(config).isSameAs(getPostgresSourceConfig());
                        return jdbc;
                    }
                };
            }
        };
    }

    private static io.debezium.relational.TableId table(String schema, String name) {
        return new io.debezium.relational.TableId("inventory", schema, name);
    }

    private static final class MetadataConnection extends JdbcConnection {
        private final Set<io.debezium.relational.TableId> tables = new HashSet<>();
        private String[] requestedTypes;
        private RuntimeException failure;
        private SQLException metadataFailure;
        private SQLException closeFailure;
        private int closeCalls;

        private MetadataConnection() {
            super(
                    JdbcConfiguration.adapt(io.debezium.config.Configuration.create().build()),
                    config -> {
                        throw new AssertionError("Unexpected database connection");
                    },
                    "\"",
                    "\"");
        }

        @Override
        public Set<io.debezium.relational.TableId> readTableNames(
                String databaseCatalog,
                String schemaPattern,
                String tablePattern,
                String[] tableTypes)
                throws SQLException {
            assertThat(databaseCatalog).isEqualTo("inventory");
            requestedTypes = tableTypes;
            if (failure != null) {
                throw failure;
            }
            if (metadataFailure != null) {
                throw metadataFailure;
            }
            return tables;
        }

        @Override
        public synchronized void close() throws SQLException {
            closeCalls++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
