/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.base;

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

import com.ververica.cdc.connectors.base.experimental.MySqlSourceBuilder;
import com.ververica.cdc.connectors.base.experimental.utils.MySqlConnectionUtils;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import com.ververica.cdc.connectors.base.testutils.MySqlContainer;
import com.ververica.cdc.connectors.base.testutils.MySqlVersion;
import com.ververica.cdc.connectors.base.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Example Tests for {@link IncrementalSource}. */
public class MySqlChangeEventSourceExampleTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(MySqlChangeEventSourceExampleTest.class);

    private static final int DEFAULT_PARALLELISM = 4;
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V5_7);

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingScanEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        MySqlSourceBuilder.MySqlIncrementalSource<String> mySqlChangeEventSource =
                new MySqlSourceBuilder<String>()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".products")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(
                        mySqlChangeEventSource,
                        WatermarkStrategy.noWatermarks(),
                        "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("weight", DataTypes.FLOAT()));

        inventoryDatabase.createAndInitialize();
        final String tableId = inventoryDatabase.getDatabaseName() + ".products";
        MySqlSourceBuilder.MySqlIncrementalSource<RowData> mySqlChangeEventSource =
                new MySqlSourceBuilder<RowData>()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(tableId)
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(buildRowDataDebeziumDeserializeSchema(dataType))
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(2)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        CloseableIterator<RowData> iterator =
                env.fromSource(
                                mySqlChangeEventSource,
                                WatermarkStrategy.noWatermarks(),
                                "MySqlParallelSource")
                        .setParallelism(4)
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

        // step-2: make 6 change events in one MySQL transaction
        makeBinlogEvents(getConnection(), tableId);

        String[] binlogExpectedRecords =
                new String[] {
                    "-U[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+U[103, cart, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+I[110, spare tire, 28 inch spare tire, 26.2]",
                    "-D[110, spare tire, 28 inch spare tire, 26.2]",
                    "-U[103, cart, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+U[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]"
                };

        // step-3: consume binlog change events
        List<RowData> binlogRowDataList = new ArrayList<>();
        for (int i = 0; i < binlogExpectedRecords.length && iterator.hasNext(); i++) {
            binlogRowDataList.add(iterator.next());
        }
        List<String> binlogActualRecords = formatResult(binlogRowDataList, dataType);
        assertEqualsInAnyOrder(Arrays.asList(binlogExpectedRecords), binlogActualRecords);

        // stop the worker
        iterator.close();
    }

    private RowDataDebeziumDeserializeSchema buildRowDataDebeziumDeserializeSchema(
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

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", inventoryDatabase.getUsername());
        properties.put("database.password", inventoryDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return MySqlConnectionUtils.createMySqlConnection(configuration);
    }

    private void makeBinlogEvents(JdbcConnection connection, String tableId) throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events
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

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withDatabaseName("flink-test")
                .withUsername("flinkuser")
                .withPassword("flinkpw")
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }
}
