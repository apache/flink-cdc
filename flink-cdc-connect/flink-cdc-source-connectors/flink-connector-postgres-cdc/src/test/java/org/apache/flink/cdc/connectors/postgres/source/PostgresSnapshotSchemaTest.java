/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests metadata loading for new and restored PostgreSQL snapshot enumerators. */
class PostgresSnapshotSchemaTest {

    private static final String DATABASE = "db";
    private static final String SCHEMA = "sales\\_%";
    private static final String SCHEMA_PATTERN = "sales" + "\\\\" + "\\_\\%";
    private static final String FIRST_TABLE_PATTERN = "orders\\_\\%\\\\live";
    private static final TableId FIRST = new TableId(null, SCHEMA, "orders_%\\live");
    private static final TableId SECOND = new TableId(null, SCHEMA, "items");
    private static final TableId PROCESSED = new TableId(null, SCHEMA, "processed");
    private static final TableId PARTITION_ROOT = new TableId(null, SCHEMA, "partition_root");
    private static final TableId OTHER_SCHEMA = new TableId(null, "archive", "items");
    private static final List<TableId> TABLES =
            List.of(FIRST, SECOND, PROCESSED, PARTITION_ROOT, OTHER_SCHEMA);

    private final List<String> tableReads = new ArrayList<>();
    private final List<List<String>> columnReads = new ArrayList<>();
    private final List<TableId> primaryKeyReads = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private final SplitEnumeratorContext<SourceSplitBase> context =
            proxy(
                    SplitEnumeratorContext.class,
                    (object, method, args) -> {
                        if (method.getName().equals("currentParallelism")) {
                            return 1;
                        }
                        return null;
                    });

    private PostgresSourceBuilder.PostgresIncrementalSource<String> source;
    private PostgresDialect dialect;
    private PostgresConnection jdbc;
    private PostgresChunkSplitter chunkSplitter;

    @BeforeEach
    void setUp() {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("localhost");
        factory.database(DATABASE);
        factory.username("user");
        factory.password("password");
        PostgresSourceConfig config = factory.create(0);
        dialect =
                new PostgresDialect(config) {
                    @Override
                    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
                        return List.of(FIRST, SECOND, OTHER_SCHEMA);
                    }

                    @Override
                    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
                        return jdbc;
                    }

                    @Override
                    public PostgresChunkSplitter createChunkSplitter(
                            JdbcSourceConfig sourceConfig, ChunkSplitterState state) {
                        chunkSplitter =
                                new PostgresChunkSplitter(sourceConfig, this, state) {
                                    @Override
                                    public Object[] queryMinMax(
                                            JdbcConnection jdbc,
                                            TableId tableId,
                                            Column splitColumn) {
                                        return new Object[] {1, 20};
                                    }

                                    @Override
                                    protected Long queryApproximateRowCnt(
                                            JdbcConnection jdbc, TableId tableId) {
                                        return 20L;
                                    }

                                    @Override
                                    public Object queryNextChunkMax(
                                            JdbcConnection jdbc,
                                            TableId tableId,
                                            Column splitColumn,
                                            int chunkSize,
                                            Object includedLowerBound) {
                                        return (Integer) includedLowerBound + 5;
                                    }
                                };
                        return chunkSplitter;
                    }
                };
        source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        factory,
                        new JsonDebeziumDeserializationSchema(),
                        new PostgresOffsetFactory(),
                        dialect);
        DatabaseMetaData metadata = proxy(DatabaseMetaData.class, this::readMetadata);
        Connection connection = proxy(Connection.class, (object, method, args) -> metadata);
        jdbc =
                new PostgresConnection(config.getDbzConnectorConfig().getJdbcConfig(), "test") {
                    @Override
                    public Connection connection() {
                        return connection;
                    }

                    @Override
                    public long currentXLogLocation() {
                        return 0;
                    }

                    @Override
                    public Long currentTransactionId() {
                        return 1L;
                    }

                    @Override
                    protected Optional<ColumnEditor> readTableColumn(
                            ResultSet row, TableId tableId, Tables.ColumnNameFilter columnFilter)
                            throws SQLException {
                        return Optional.of(
                                Column.editor()
                                        .name(row.getString(4))
                                        .type("int4")
                                        .jdbcType(Types.INTEGER)
                                        .position(1));
                    }
                };
    }

    @Test
    void loadsEachInitialSchemaOnItsFirstTable() throws Exception {
        try (PostgresSourceEnumerator enumerator = source.createEnumerator(context)) {
            assertThat(tableReads).isEmpty();

            assertThat(dialect.queryTableSchema(jdbc, FIRST).getTable().primaryKeyColumnNames())
                    .containsExactly("id");
            assertThat(tableReads).containsExactly(SCHEMA_PATTERN);
            assertThat(columnReads).containsExactly(Arrays.asList(SCHEMA_PATTERN, null));
            assertThat(primaryKeyReads).containsExactlyInAnyOrder(FIRST, SECOND);

            assertThat(dialect.queryTableSchema(jdbc, SECOND).getId()).isEqualTo(SECOND);
            assertThat(tableReads).containsExactly(SCHEMA_PATTERN);
            assertThat(columnReads).hasSize(1);
            assertThat(primaryKeyReads).hasSize(2);

            assertThat(dialect.queryTableSchema(jdbc, OTHER_SCHEMA).getId())
                    .isEqualTo(OTHER_SCHEMA);
            assertThat(tableReads).containsExactly(SCHEMA_PATTERN, "archive");
            assertThat(columnReads)
                    .containsExactly(
                            Arrays.asList(SCHEMA_PATTERN, null), Arrays.asList("archive", null));
            assertThat(primaryKeyReads).containsExactlyInAnyOrder(FIRST, SECOND, OTHER_SCHEMA);
        }
    }

    @Test
    void loadsOnlyTheRequestedTableAfterRecovery() throws Exception {
        try (PostgresSourceEnumerator enumerator =
                source.restoreEnumerator(
                        context, restoredCheckpoint(ChunkSplitterState.NO_SPLITTING_TABLE_STATE))) {
            assertThat(tableReads).isEmpty();

            assertThat(dialect.queryTableSchema(jdbc, FIRST).getId()).isEqualTo(FIRST);
            assertThat(dialect.queryTableSchema(jdbc, FIRST).getId()).isEqualTo(FIRST);
            assertThat(tableReads).containsExactly(SCHEMA_PATTERN);
            assertThat(columnReads).containsExactly(List.of(SCHEMA_PATTERN, FIRST_TABLE_PATTERN));
            assertThat(primaryKeyReads).containsExactly(FIRST);

            assertThat(dialect.queryTableSchema(jdbc, SECOND).getId()).isEqualTo(SECOND);
            assertThat(tableReads).containsExactly(SCHEMA_PATTERN, SCHEMA_PATTERN);
            assertThat(columnReads)
                    .containsExactly(
                            List.of(SCHEMA_PATTERN, FIRST_TABLE_PATTERN),
                            List.of(SCHEMA_PATTERN, SECOND.table()));
            assertThat(primaryKeyReads).containsExactly(FIRST, SECOND);
        }
    }

    @Test
    void resumesPartiallySplitTableAfterCheckpointRoundTrip() throws Exception {
        ChunkSplitterState savedState =
                new ChunkSplitterState(FIRST, ChunkSplitterState.ChunkBound.middleOf(5), 2);
        try (PostgresSourceEnumerator enumerator =
                source.restoreEnumerator(context, restoredCheckpoint(savedState))) {
            assertThat(tableReads).isEmpty();
            assertThat(chunkSplitter.getCurrentSplittingTableId().schema()).isEqualTo(SCHEMA);

            chunkSplitter.open();
            assertThat(chunkSplitter.generateSplits(chunkSplitter.getCurrentSplittingTableId()))
                    .singleElement()
                    .satisfies(
                            split -> {
                                assertThat(split.splitId()).isEqualTo(FIRST + ":2");
                                assertThat(split.getSplitStart()).containsExactly(5);
                                assertThat(split.getSplitEnd()).containsExactly(10);
                            });
            assertThat(tableReads).containsExactly(SCHEMA_PATTERN);
            assertThat(columnReads).containsExactly(List.of(SCHEMA_PATTERN, FIRST_TABLE_PATTERN));
            assertThat(primaryKeyReads).containsExactly(FIRST);
        }
    }

    private PendingSplitsState restoredCheckpoint(ChunkSplitterState chunkState)
            throws IOException {
        SnapshotPendingSplitsState snapshot =
                new SnapshotPendingSplitsState(
                        List.of(PROCESSED, OTHER_SCHEMA),
                        List.of(),
                        Map.of(),
                        Map.of(),
                        Map.of(),
                        AssignerStatus.INITIAL_ASSIGNING,
                        List.of(FIRST, SECOND),
                        true,
                        true,
                        Map.of(),
                        chunkState);
        SimpleVersionedSerializer<PendingSplitsState> serializer =
                source.getEnumeratorCheckpointSerializer();
        return serializer.deserialize(
                serializer.getVersion(),
                serializer.serialize(new HybridPendingSplitsState(snapshot, false)));
    }

    @Test
    void reportsTheMissingRequestedTable() {
        TableId missing = new TableId(null, SCHEMA, "missing");

        assertThatThrownBy(() -> dialect.queryTableSchema(jdbc, missing))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Failed to read table schema of table %s", missing);
        assertThat(tableReads).containsExactly(SCHEMA_PATTERN);
        assertThat(columnReads).isEmpty();
        assertThat(primaryKeyReads).isEmpty();
    }

    private Object readMetadata(Object object, Method method, Object[] args) {
        switch (method.getName()) {
            case "getSearchStringEscape":
                return "\\";
            case "getTables":
                tableReads.add((String) args[1]);
                return rows((String) args[1], null, "TABLE");
            case "getColumns":
                columnReads.add(Arrays.asList((String) args[1], (String) args[2]));
                return rows((String) args[1], (String) args[2], "id");
            case "getPrimaryKeys":
                TableId tableId = new TableId((String) args[0], (String) args[1], (String) args[2]);
                primaryKeyReads.add(tableId);
                return rows(List.of(tableId), "id");
            default:
                throw new UnsupportedOperationException(method.getName());
        }
    }

    private static ResultSet rows(String schemaPattern, String tablePattern, String field) {
        String schema = SCHEMA_PATTERN.equals(schemaPattern) ? SCHEMA : schemaPattern;
        String table = FIRST_TABLE_PATTERN.equals(tablePattern) ? FIRST.table() : tablePattern;
        return rows(
                TABLES.stream()
                        .filter(id -> schema == null || schema.equals(id.schema()))
                        .filter(id -> table == null || table.equals(id.table()))
                        .collect(Collectors.toList()),
                field);
    }

    private static ResultSet rows(List<TableId> tableIds, String field) {
        Iterator<TableId> ids = tableIds.iterator();
        return proxy(
                ResultSet.class,
                new InvocationHandler() {
                    private TableId current;

                    @Override
                    public Object invoke(Object object, Method method, Object[] args) {
                        switch (method.getName()) {
                            case "next":
                                current = ids.hasNext() ? ids.next() : null;
                                return current != null;
                            case "getString":
                                switch ((int) args[0]) {
                                    case 1:
                                        return current.catalog();
                                    case 2:
                                        return current.schema();
                                    case 3:
                                        return current.table();
                                    case 4:
                                        return "TABLE".equals(field)
                                                        && PARTITION_ROOT.equals(current)
                                                ? "PARTITIONED TABLE"
                                                : field;
                                    default:
                                        throw new UnsupportedOperationException();
                                }
                            case "getInt":
                                return 1;
                            case "close":
                                return null;
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    }
                });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(
                Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }
}
