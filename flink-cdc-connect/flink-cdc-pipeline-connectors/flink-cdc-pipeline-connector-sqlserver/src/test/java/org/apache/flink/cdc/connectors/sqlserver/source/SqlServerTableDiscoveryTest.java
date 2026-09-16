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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlServerTableDiscoveryTest {
    @Test
    void filtersCapturedTablesAndPreservesTableIdentifiers() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.tables.addAll(
                Arrays.asList(
                        new String[] {"inventory", "dbo", "orders_1"},
                        new String[] {"inventory", "dbo", "orders$2026"},
                        new String[] {"inventory", "dbo", "orders_archive"},
                        new String[] {"inventory", "dbo", "customers"},
                        new String[] {"inventory", "sales", "orders_1"}));

        assertThat(source(jdbc).listCapturedTables())
                .containsExactlyInAnyOrder(
                        TableId.tableId("inventory", "dbo", "orders_1"),
                        TableId.tableId("inventory", "dbo", "orders$2026"));
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void returnsEmptyWhenMetadataIsEmptyOrAllTablesAreFilteredOut(boolean hasTables) {
        MetadataConnection jdbc = new MetadataConnection();
        if (hasTables) {
            jdbc.tables.addAll(
                    Arrays.asList(
                            new String[] {"inventory", "dbo", "customers"},
                            new String[] {"inventory", "dbo", "orders_archive"}));
        }
        assertThat(source(jdbc).listCapturedTables()).isEmpty();
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void propagatesRuntimeMetadataFailuresAndClosesTheConnection() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.failure = new IllegalStateException("metadata unavailable");
        SqlServerDataSource source = source(jdbc);

        assertThatThrownBy(source::listCapturedTables).isSameAs(jdbc.failure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void wrapsDatabaseQuerySqlExceptionAndClosesTheConnection() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.databaseQueryFailure = new SQLException("database metadata unavailable");
        SqlServerDataSource source = source(jdbc);

        assertThatThrownBy(source::listCapturedTables)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasCause(jdbc.databaseQueryFailure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void returnsEmptyWhenTableQueryThrowsSqlExceptionAndClosesTheConnection() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.tables.add(new String[] {"inventory", "dbo", "orders_1"});
        jdbc.tableQueryFailure = new SQLException("table metadata unavailable");

        assertThat(source(jdbc).listCapturedTables()).isEmpty();
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void wrapsConnectionCloseSqlException() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.closeFailure = new SQLException("connection close failed");
        SqlServerDataSource source = source(jdbc);

        assertThatThrownBy(source::listCapturedTables)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasCause(jdbc.closeFailure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    private static SqlServerDataSource source(MetadataConnection jdbc) {
        SqlServerSourceConfigFactory factory = new SqlServerSourceConfigFactory();
        factory.hostname("localhost")
                .port(1433)
                .username("test")
                .password("test")
                .databaseList("inventory")
                .tableList("dbo\\.orders_1", "dbo\\.orders\\$2026");

        return new SqlServerDataSource(factory) {
            @Override
            SqlServerDialect createTableDiscoveryDialect() {
                return new SqlServerDialect(getSqlServerSourceConfig()) {
                    @Override
                    public JdbcConnection openJdbcConnection(JdbcSourceConfig config) {
                        assertThat(config).isSameAs(getSqlServerSourceConfig());
                        return jdbc;
                    }
                };
            }
        };
    }

    private static CachedRowSet rows(int columns, List<String[]> values) throws SQLException {
        CachedRowSet rows = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(columns);
        for (int i = 1; i <= columns; i++) {
            metadata.setColumnType(i, Types.VARCHAR);
            metadata.setColumnName(i, "column" + i);
        }
        rows.setMetaData(metadata);
        for (String[] value : values) {
            rows.moveToInsertRow();
            for (int i = 0; i < value.length; i++) {
                rows.updateString(i + 1, value[i]);
            }
            rows.insertRow();
            rows.moveToCurrentRow();
        }
        rows.beforeFirst();
        return rows;
    }

    private static final class MetadataConnection extends JdbcConnection {
        private final List<String[]> tables = new ArrayList<>();
        private RuntimeException failure;
        private SQLException databaseQueryFailure;
        private SQLException tableQueryFailure;
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
        public JdbcConnection query(String query, ResultSetConsumer consumer) throws SQLException {
            if (failure != null) {
                throw failure;
            }
            if (query.contains("FROM sys.databases")) {
                if (databaseQueryFailure != null) {
                    throw databaseQueryFailure;
                }
                try (CachedRowSet rows =
                        rows(
                                1,
                                Arrays.asList(
                                        new String[] {"inventory"}, new String[] {"other"}))) {
                    consumer.accept(rows);
                }
            } else {
                assertThat(query).contains("[inventory].INFORMATION_SCHEMA.TABLES");
                if (tableQueryFailure != null) {
                    throw tableQueryFailure;
                }
                try (CachedRowSet rows = rows(3, tables)) {
                    consumer.accept(rows);
                }
            }
            return this;
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
