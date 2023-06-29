/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.postgres.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.PostgresTestBase;
import com.ververica.cdc.connectors.postgres.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Tests for Postgres Source based on incremental snapshot framework . */
public class PostgresSourceExampleTest extends PostgresTestBase {

    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "inventory";
    private static final String TABLE_ID = SCHEMA_NAME + ".products";

    private static final String SLOT_NAME = "flink";
    private static final String PLUGIN_NAME = "decoderbufs";
    private static final long CHECKPOINT_INTERVAL_MS = 3000;

    private static final int DEFAULT_PARALLELISM = 2;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    // 9 records in the inventory.products table
    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingScanEvents() throws Exception {

        inventoryDatabase.createAndInitialize();

        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema();

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                        .database(inventoryDatabase.getDatabaseName())
                        .schemaList(SCHEMA_NAME)
                        .tableList(TABLE_ID)
                        .username(POSTGRES_CONTAINER.getUsername())
                        .password(POSTGRES_CONTAINER.getPassword())
                        .slotName(SLOT_NAME)
                        .decodingPluginName(PLUGIN_NAME)
                        .deserializer(deserializer)
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(2)
                        .build();

        // The splitSize 2 will split the data into 5 chunks for 9 records
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2)
                .print();

        env.execute("Output Postgres Snapshot");
    }

    @Test
    @Ignore
    public void testConsumingAllEvents() throws Exception {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("weight", DataTypes.FLOAT()));

        inventoryDatabase.createAndInitialize();
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("snapshot.mode", "never");

        JdbcIncrementalSource<RowData> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<RowData>builder()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                        .database(inventoryDatabase.getDatabaseName())
                        .schemaList(SCHEMA_NAME)
                        .tableList(TABLE_ID)
                        .username(POSTGRES_CONTAINER.getUsername())
                        .password(POSTGRES_CONTAINER.getPassword())
                        .slotName(SLOT_NAME)
                        .decodingPluginName(PLUGIN_NAME)
                        .deserializer(buildRowDataDebeziumDeserializeSchema(dataType))
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(2)
                        .debeziumProperties(debeziumProps)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        CloseableIterator<RowData> iterator =
                env.fromSource(
                                postgresIncrementalSource,
                                WatermarkStrategy.noWatermarks(),
                                "PostgresParallelSource")
                        .setParallelism(2)
                        .executeAndCollect(); // collect record

        String[] snapshotExpectedRecords =
                new String[] {
                    "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                    "+I[102, car battery, 12V car battery, 8.1]",
                    "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+I[104, hammer, 12oz carpenter's hammer, 0.75]",
                    "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                    "+I[106, hammer, 16oz carpenter's hammer, 1.0]",
                    "+I[107, rocks, box of assorted rocks, 5.3]",
                    "+I[108, jacket, water resistent black wind breaker, 0.1]",
                    "+I[109, spare tire, 24 inch spare tire, 22.2]"
                };

        // step-1: consume snapshot data
        List<RowData> snapshotRowDataList = new ArrayList<>();
        for (int i = 0; i < snapshotExpectedRecords.length && iterator.hasNext(); i++) {
            snapshotRowDataList.add(iterator.next());
        }

        List<String> snapshotActualRecords = formatResult(snapshotRowDataList, dataType);
        assertEqualsInAnyOrder(Arrays.asList(snapshotExpectedRecords), snapshotActualRecords);

        log.info("All snapshot data consumed!");

        // step-2: make 6 change events in one PostgreSQL transaction
        makeWalEvents(getConnection(), TABLE_ID);

        String[] walExpectedRecords =
                new String[] {
                    "-U[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+U[103, cart, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+I[110, spare tire, 28 inch spare tire, 26.2]",
                    "-D[110, spare tire, 28 inch spare tire, 26.2]",
                    "-U[103, cart, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+U[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]"
                };

        // step-3: consume wal events
        List<RowData> walRowDataList = new ArrayList<>();
        for (int i = 0; i < walExpectedRecords.length && iterator.hasNext(); i++) {
            RowData rowData = iterator.next();
            log.info("step 3: consume wal event: {}", rowData);
            walRowDataList.add(rowData);
        }

        List<String> walActualRecords = formatResult(walRowDataList, dataType);
        assertEqualsInAnyOrder(Arrays.asList(walExpectedRecords), walActualRecords);

        log.info("All streaming events consumed!");

        // stop the worker
        iterator.close();
    }

    private DebeziumDeserializationSchema<RowData> buildRowDataDebeziumDeserializeSchema(
            DataType dataType) {
        LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);
        return RowDataDebeziumDeserializeSchema.newBuilder()
                .setPhysicalRowType((RowType) dataType.getLogicalType())
                .setResultTypeInfo(typeInfo)
                .build();
    }

    private List<String> formatResult(List<RowData> records, DataType dataType) {
        RowRowConverter rowRowConverter = RowRowConverter.create(dataType);
        rowRowConverter.open(Thread.currentThread().getContextClassLoader());
        return records.stream()
                .map(rowRowConverter::toExternal)
                .map(Object::toString)
                .collect(Collectors.toList());
    }

    private PostgresConnection getConnection() throws SQLException {

        Map<String, String> properties = new HashMap<>();
        properties.put("hostname", POSTGRES_CONTAINER.getHost());
        properties.put("port", String.valueOf(POSTGRES_CONTAINER.getMappedPort(POSTGRESQL_PORT)));
        properties.put("dbname", inventoryDatabase.getDatabaseName());
        properties.put("user", inventoryDatabase.getUsername());
        properties.put("password", inventoryDatabase.getPassword());
        PostgresConnection connection = createConnection(properties);
        connection.connect();
        return connection;
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }

    private void makeWalEvents(PostgresConnection connection, String tableId) throws SQLException {

        waitForReplicationSlotReady(connection);

        try {
            connection.setAutoCommit(false);

            // make WAL events
            connection.execute(
                    "UPDATE " + tableId + " SET name = 'cart' where id = 103",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(110,'spare tire','28 inch spare tire','26.2')",
                    "DELETE FROM " + tableId + " where id = 110",
                    "UPDATE " + tableId + " SET name = '12-pack drill bits' where id = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void waitForReplicationSlotReady(PostgresConnection connection) throws SQLException {
        SlotState slotState = connection.getReplicationSlotState(SLOT_NAME, PLUGIN_NAME);

        while (slotState == null) {
            log.info("Waiting until the replication slot is ready ...");
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            slotState = connection.getReplicationSlotState(SLOT_NAME, PLUGIN_NAME);
        }
    }
}
