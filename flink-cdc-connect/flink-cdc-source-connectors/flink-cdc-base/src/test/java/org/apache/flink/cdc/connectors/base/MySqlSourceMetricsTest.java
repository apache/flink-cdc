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

package org.apache.flink.cdc.connectors.base;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.experimental.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.base.source.MySqlEventDeserializer;
import org.apache.flink.cdc.connectors.base.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.base.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.base.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.junit.Assert;
import org.junit.BeforeClass;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** MySQL Source Metrics Tests. */
public class MySqlSourceMetricsTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceMetricsTest.class);

    private static final int DEFAULT_PARALLELISM = 4;
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V5_7);
    protected InMemoryReporter metricReporter = InMemoryReporter.createWithRetainedMetrics();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .setConfiguration(
                                    metricReporter.addToConfiguration(new Configuration()))
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "metrics", "mysqluser", "mysqlpw");

    @Test
    public void testSourceMetrics() throws Exception {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT()));

        inventoryDatabase.createAndInitialize();
        final String tableId = inventoryDatabase.getDatabaseName() + ".users";
        MySqlSourceBuilder.MySqlIncrementalSource<Event> mySqlChangeEventSource =
                new MySqlSourceBuilder<Event>()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(tableId)
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .serverId("5401-5404")
                        .deserializer(buildRowDataDebeziumDeserializeSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(2)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        CloseableIterator<Event> iterator =
                env.fromSource(
                                mySqlChangeEventSource,
                                WatermarkStrategy.noWatermarks(),
                                "MySqlParallelSource")
                        .setParallelism(1)
                        .executeAndCollect(); // collect record
        String[] snapshotExpectedRecords =
                new String[] {
                    "+I[101, Tom, 3]",
                    "+I[102, Jack, 5]",
                    "+I[103, Allen, 10]",
                    "+I[104, Andrew, 13]",
                    "+I[105, Arnold, 15]",
                    "+I[106, Claud, 19]",
                    "+I[107, Howard, 37]",
                    "+I[108, Jacob, 46]",
                    "+I[109, Lionel, 58]"
                };

        // step-1: consume snapshot data
        List<Event> snapshotRowDataList = new ArrayList<>();
        for (int i = 0; i < snapshotExpectedRecords.length && iterator.hasNext(); i++) {
            snapshotRowDataList.add(iterator.next());
        }

        List<String> snapshotActualRecords = formatResult(snapshotRowDataList, dataType);
        assertEqualsInAnyOrder(Arrays.asList(snapshotExpectedRecords), snapshotActualRecords);

        // step-2: make 6 change events in one MySQL transaction
        makeBinlogEvents(getConnection(), tableId);
        // mock ddl events
        makeDdlEvents(getConnection(), tableId);

        String[] binlogExpectedRecords =
                new String[] {
                    "-U[103, Allen, 10]",
                    "+U[103, Oswald, 10]",
                    "+I[110, Terence, 78]",
                    "-D[110, Terence, 78]",
                    "-U[103, Oswald, 10]",
                    "+U[103, Marry, 10]"
                };

        // step-3: consume binlog change events
        List<Event> binlogRowDataList = new ArrayList<>();
        for (int i = 0; i < 4 && iterator.hasNext(); i++) {
            binlogRowDataList.add(iterator.next());
        }
        List<String> binlogActualRecords = formatResult(binlogRowDataList, dataType);
        assertEqualsInAnyOrder(Arrays.asList(binlogExpectedRecords), binlogActualRecords);

        Set<MetricGroup> metricGroups = metricReporter.findGroups("users");
        for (MetricGroup enumeratorGroup : metricGroups) {
            boolean isTableMetric = true;
            for (String scopeComponent : enumeratorGroup.getScopeComponents()) {
                if (scopeComponent.contains("enumerator")) {
                    isTableMetric = false;
                    break;
                }
            }
            if (!isTableMetric) {
                break;
            }
            Map<String, Metric> enumeratorMetrics =
                    metricReporter.getMetricsByGroup(enumeratorGroup);
            Assert.assertEquals(
                    1, ((Counter) enumeratorMetrics.get("numDeleteDMLRecords")).getCount());
            Assert.assertEquals(
                    1, ((Counter) enumeratorMetrics.get("numInsertDMLRecords")).getCount());
            Assert.assertEquals(
                    9, ((Counter) enumeratorMetrics.get("numSnapshotRecords")).getCount());
            // ddl eventd
            Assert.assertEquals(1, ((Counter) enumeratorMetrics.get("numDDLRecords")).getCount());
            Assert.assertEquals(13, ((Counter) enumeratorMetrics.get("numRecordsIn")).getCount());
            Assert.assertEquals(
                    2, ((Counter) enumeratorMetrics.get("numUpdateDMLRecords")).getCount());
        }
        Set<MetricGroup> enumeratorGroups = metricReporter.findGroups("enumerator");
        for (MetricGroup enumeratorGroup : enumeratorGroups) {
            boolean isTableMetric = false;
            for (String scopeComponent : enumeratorGroup.getScopeComponents()) {
                if (scopeComponent.contains("users")) {
                    isTableMetric = true;
                    break;
                }
            }
            Map<String, Metric> enumeratorMetrics =
                    metricReporter.getMetricsByGroup(enumeratorGroup);
            if (isTableMetric) {
                Assert.assertEquals(
                        0,
                        ((Gauge<Integer>) enumeratorMetrics.get("numSnapshotSplitsRemaining"))
                                .getValue()
                                .intValue());
                Assert.assertEquals(
                        5,
                        ((Gauge<Integer>) enumeratorMetrics.get("numSnapshotSplitsProcessed"))
                                .getValue()
                                .intValue());
                Assert.assertEquals(
                        5,
                        ((Gauge<Integer>) enumeratorMetrics.get("numSnapshotSplitsFinished"))
                                .getValue()
                                .intValue());
                Assert.assertTrue(
                        ((Gauge<Long>) enumeratorMetrics.get("snapshotEndTime"))
                                        .getValue()
                                        .longValue()
                                > 0);
                Assert.assertTrue(
                        ((Gauge<Long>) enumeratorMetrics.get("snapshotStartTime"))
                                        .getValue()
                                        .longValue()
                                > 0);
            } else {
                Assert.assertEquals(
                        0,
                        ((Gauge<Integer>) enumeratorMetrics.get("isSnapshotting"))
                                .getValue()
                                .intValue());
                Assert.assertEquals(
                        1,
                        ((Gauge<Integer>) enumeratorMetrics.get("isStreamReading"))
                                .getValue()
                                .intValue());
                Assert.assertEquals(
                        1,
                        ((Gauge<Integer>) enumeratorMetrics.get("numTablesSnapshotted"))
                                .getValue()
                                .intValue());
                Assert.assertEquals(
                        0,
                        ((Gauge<Integer>) enumeratorMetrics.get("numSnapshotSplitsRemaining"))
                                .getValue()
                                .intValue());
                Assert.assertEquals(
                        5,
                        ((Gauge<Integer>) enumeratorMetrics.get("numSnapshotSplitsProcessed"))
                                .getValue()
                                .intValue());
            }
        }
        // stop the worker
        iterator.close();
    }

    private MySqlEventDeserializer buildRowDataDebeziumDeserializeSchema() {
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(DebeziumChangelogMode.ALL, true, false);
        return deserializer;
    }

    private List<String> formatResult(List<Event> records, DataType dataType) {
        RowRowConverter rowRowConverter = RowRowConverter.create(dataType);
        rowRowConverter.open(Thread.currentThread().getContextClassLoader());
        return records.stream()
                .flatMap(
                        item -> {
                            DataChangeEvent changeEvent = ((DataChangeEvent) item);
                            RecordData before = changeEvent.before();
                            RecordData after = changeEvent.after();

                            switch (changeEvent.op()) {
                                case INSERT:
                                    GenericRowData insertData = new GenericRowData(3);
                                    insertData.setRowKind(RowKind.INSERT);
                                    convertData(changeEvent.after(), insertData);
                                    return Arrays.stream(new GenericRowData[] {insertData});
                                case DELETE:
                                    GenericRowData deleteData = null;
                                    deleteData = new GenericRowData(3);
                                    deleteData.setRowKind(RowKind.DELETE);
                                    convertData(before, deleteData);
                                    return Arrays.stream(new GenericRowData[] {deleteData});
                                case UPDATE:
                                case REPLACE:
                                    GenericRowData beforeData = new GenericRowData(3);
                                    beforeData.setRowKind(RowKind.UPDATE_BEFORE);
                                    convertData(before, beforeData);

                                    GenericRowData afterData = new GenericRowData(3);
                                    afterData.setRowKind(RowKind.UPDATE_AFTER);
                                    convertData(after, afterData);
                                    return Stream.of(beforeData, afterData)
                                            .filter(row -> row != null);
                            }
                            return Stream.empty();
                        })
                .map(rowRowConverter::toExternal)
                .map(Object::toString)
                .collect(Collectors.toList());
    }

    private void convertData(RecordData inputData, GenericRowData outputData) {
        outputData.setField(0, inputData.getLong(0));
        outputData.setField(1, StringData.fromString(inputData.getString(1).toString()));
        outputData.setField(2, inputData.getInt(2));
    }

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", inventoryDatabase.getUsername());
        properties.put("database.password", inventoryDatabase.getPassword());
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        // properties.put("transaction.topic", "transaction_topic");
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return new MySqlConnection(new MySqlConnection.MySqlConnectionConfiguration(configuration));
    }

    private void makeBinlogEvents(JdbcConnection connection, String tableId) throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events
            connection.execute(
                    "UPDATE " + tableId + " SET name = 'Oswald' where id = 103",
                    "INSERT INTO " + tableId + " VALUES(110,'Terence',78)",
                    "DELETE FROM " + tableId + " where id = 110",
                    "UPDATE " + tableId + " SET name = 'Marry' where id = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeDdlEvents(JdbcConnection connection, String tableId) throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events
            connection.execute("alter table " + tableId + " add test_add_col int null");
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
