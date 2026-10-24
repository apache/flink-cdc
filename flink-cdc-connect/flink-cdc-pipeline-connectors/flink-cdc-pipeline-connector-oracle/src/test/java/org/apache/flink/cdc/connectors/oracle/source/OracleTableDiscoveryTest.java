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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OracleTableDiscoveryTest {
    @Test
    void filtersCapturedTablesAndPreservesTableIdentifiers() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.tables.addAll(
                Arrays.asList(
                        new String[] {"SALES", "ORDERS_1"},
                        new String[] {"SALES", "ORDERS$2026"},
                        new String[] {"SALES", "ORDERS_archive"},
                        new String[] {"SALES", "CUSTOMERS"},
                        new String[] {"OTHER", "ORDERS_1"}));

        assertThat(source(jdbc).listCapturedTables())
                .containsExactlyInAnyOrder(
                        TableId.tableId("SALES", "ORDERS_1"),
                        TableId.tableId("SALES", "ORDERS$2026"));
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void returnsEmptyWhenMetadataIsEmptyOrAllTablesAreFilteredOut(boolean hasTables) {
        MetadataConnection jdbc = new MetadataConnection();
        if (hasTables) {
            jdbc.tables.addAll(
                    Arrays.asList(
                            new String[] {"SALES", "CUSTOMERS"},
                            new String[] {"SALES", "ORDERS_archive"}));
        }
        assertThat(source(jdbc).listCapturedTables()).isEmpty();
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void propagatesRuntimeMetadataFailuresAndClosesTheConnection() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.failure = new IllegalStateException("metadata unavailable");
        OracleDataSource source = source(jdbc);

        assertThatThrownBy(source::listCapturedTables).isSameAs(jdbc.failure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void returnsEmptyWhenTableQueryThrowsSqlExceptionAndClosesTheConnection() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.tables.add(new String[] {"SALES", "ORDERS_1"});
        jdbc.queryFailure = new SQLException("table metadata unavailable");

        assertThat(source(jdbc).listCapturedTables()).isEmpty();
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    @Test
    void wrapsConnectionCloseSqlException() {
        MetadataConnection jdbc = new MetadataConnection();
        jdbc.closeFailure = new SQLException("connection close failed");
        OracleDataSource source = source(jdbc);

        assertThatThrownBy(source::listCapturedTables)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasCause(jdbc.closeFailure);
        assertThat(jdbc.closeCalls).isEqualTo(1);
    }

    private static OracleDataSource source(MetadataConnection jdbc) {
        OracleSourceConfigFactory factory = new OracleSourceConfigFactory();
        factory.hostname("localhost")
                .port(1521)
                .username("test")
                .password("test")
                .databaseList("ORCLCDB")
                .tableList("SALES\\.ORDERS_1", "SALES\\.ORDERS\\$2026");
        factory.schemaList("SALES");
        return new OracleDataSource(factory, new Configuration(), Collections.emptyList()) {
            @Override
            OracleDialect createTableDiscoveryDialect() {
                return new OracleDialect() {
                    @Override
                    public JdbcConnection openJdbcConnection(JdbcSourceConfig config) {
                        assertThat(config).isSameAs(getSourceConfig());
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
        private SQLException queryFailure;
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
        public String database() {
            return "ORCLCDB";
        }

        @Override
        public JdbcConnection query(String query, ResultSetConsumer consumer) throws SQLException {
            if (failure != null) {
                throw failure;
            }
            assertThat(query).contains("FROM ALL_TABLES");
            if (queryFailure != null) {
                throw queryFailure;
            }
            try (CachedRowSet rows = rows(2, tables)) {
                consumer.accept(rows);
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
