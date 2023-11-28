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

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.table.DebeziumChangelogMode;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import io.debezium.jdbc.JdbcConnection;
import org.junit.After;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MySqlSource} with {@link MySqlEventDeserializer}. */
public class MySqlEventSourceITCase extends MySqlSourceTestBase {

    private final BinaryRecordDataGenerator generator =
            new BinaryRecordDataGenerator(
                    RowType.of(
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING()));

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @After
    public void clear() {
        customerDatabase.dropDatabase();
    }

    @Test
    public void testProduceDataChangeEvents() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        customerDatabase.createAndInitialize();

        MySqlSourceConfig sourceConfig = getConfig(new String[] {"customers"});
        MySqlSource<Event> eventSource = getMySqlSource(sourceConfig);

        DataStreamSource<Event> source =
                env.fromSource(eventSource, WatermarkStrategy.noWatermarks(), "Event-Source");

        CloseableIterator<Event> iterator = source.collectAsync();
        env.executeAsync();

        TableId tableId = TableId.tableId(customerDatabase.getDatabaseName(), "customers");

        List<BinaryRecordData> snapshotRecords =
                customerRecordData(
                        Arrays.asList(
                                new Object[] {101, "user_1", "Shanghai", "123567891234"},
                                new Object[] {102, "user_2", "Shanghai", "123567891234"},
                                new Object[] {103, "user_3", "Shanghai", "123567891234"},
                                new Object[] {109, "user_4", "Shanghai", "123567891234"},
                                new Object[] {110, "user_5", "Shanghai", "123567891234"},
                                new Object[] {111, "user_6", "Shanghai", "123567891234"},
                                new Object[] {118, "user_7", "Shanghai", "123567891234"},
                                new Object[] {121, "user_8", "Shanghai", "123567891234"},
                                new Object[] {123, "user_9", "Shanghai", "123567891234"},
                                new Object[] {1009, "user_10", "Shanghai", "123567891234"},
                                new Object[] {1010, "user_11", "Shanghai", "123567891234"},
                                new Object[] {1011, "user_12", "Shanghai", "123567891234"},
                                new Object[] {1012, "user_13", "Shanghai", "123567891234"},
                                new Object[] {1013, "user_14", "Shanghai", "123567891234"},
                                new Object[] {1014, "user_15", "Shanghai", "123567891234"},
                                new Object[] {1015, "user_16", "Shanghai", "123567891234"},
                                new Object[] {1016, "user_17", "Shanghai", "123567891234"},
                                new Object[] {1017, "user_18", "Shanghai", "123567891234"},
                                new Object[] {1018, "user_19", "Shanghai", "123567891234"},
                                new Object[] {1019, "user_20", "Shanghai", "123567891234"},
                                new Object[] {2000, "user_21", "Shanghai", "123567891234"}));

        List<DataChangeEvent> expectedSnapshotResults =
                snapshotRecords.stream()
                        .map(
                                record ->
                                        DataChangeEvent.insertEvent(
                                                tableId, record, Collections.emptyMap()))
                        .collect(Collectors.toList());

        List<Event> snapshotResults = fetchResults(iterator, expectedSnapshotResults.size());
        assertThat(snapshotResults).containsExactlyInAnyOrderElementsOf(expectedSnapshotResults);

        try (JdbcConnection connection = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            connection.setAutoCommit(false);
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','Shanghai','123567891234'),"
                            + " (2002, 'user_23','Shanghai','123567891234'),"
                            + "(2003, 'user_24','Shanghai','123567891234')");
            connection.execute("DELETE FROM " + tableId + " where id = 101");
            connection.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();
        }

        List<Event> expectedStreamRecords = new ArrayList<>();

        List<BinaryRecordData> insertedRecords =
                customerRecordData(
                        Arrays.asList(
                                new Object[] {2001, "user_22", "Shanghai", "123567891234"},
                                new Object[] {2002, "user_23", "Shanghai", "123567891234"},
                                new Object[] {2003, "user_24", "Shanghai", "123567891234"}));
        expectedStreamRecords.addAll(
                insertedRecords.stream()
                        .map(
                                record ->
                                        DataChangeEvent.insertEvent(
                                                tableId, record, Collections.emptyMap()))
                        .collect(Collectors.toList()));

        BinaryRecordData deletedRecord =
                customerRecordData(new Object[] {101, "user_1", "Shanghai", "123567891234"});
        expectedStreamRecords.add(
                DataChangeEvent.deleteEvent(tableId, deletedRecord, Collections.emptyMap()));

        BinaryRecordData updateBeforeRecord =
                customerRecordData(new Object[] {1010, "user_11", "Shanghai", "123567891234"});
        BinaryRecordData updateAfterRecord =
                customerRecordData(new Object[] {1010, "user_11", "Hangzhou", "123567891234"});
        expectedStreamRecords.add(
                DataChangeEvent.updateEvent(
                        tableId, updateBeforeRecord, updateAfterRecord, Collections.emptyMap()));

        List<Event> streamResults = fetchResults(iterator, expectedStreamRecords.size());
        assertThat(streamResults).isEqualTo(expectedStreamRecords);
    }

    private MySqlSource<Event> getMySqlSource(MySqlSourceConfig sourceConfig) {
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        ZoneId.of(sourceConfig.getServerTimeZone()),
                        sourceConfig.isIncludeSchemaChanges());

        return MySqlSource.<Event>builder()
                .hostname(sourceConfig.getHostname())
                .port(sourceConfig.getPort())
                .databaseList(sourceConfig.getDatabaseList().toArray(new String[0]))
                .tableList(sourceConfig.getTableList().toArray(new String[0]))
                .username(sourceConfig.getUsername())
                .password(sourceConfig.getPassword())
                .deserializer(deserializer)
                .serverTimeZone(sourceConfig.getServerTimeZone())
                .debeziumProperties(sourceConfig.getDbzProperties())
                .build();
    }

    private MySqlSourceConfig getConfig(String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.latest())
                .databaseList(customerDatabase.getDatabaseName())
                .tableList(captureTableIds)
                .includeSchemaChanges(false)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .splitSize(10)
                .fetchSize(2)
                .username(customerDatabase.getUsername())
                .password(customerDatabase.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .createConfig(0);
    }

    private List<BinaryRecordData> customerRecordData(List<Object[]> records) {
        return records.stream().map(this::customerRecordData).collect(Collectors.toList());
    }

    private BinaryRecordData customerRecordData(Object[] record) {
        return generator.generate(
                new Object[] {
                    record[0],
                    BinaryStringData.fromString((String) record[1]),
                    BinaryStringData.fromString((String) record[2]),
                    BinaryStringData.fromString((String) record[3])
                });
    }

    private static List<Event> fetchResults(Iterator<Event> iter, int size) {
        List<Event> result = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Event event = iter.next();
            result.add(event);
            size--;
        }
        return result;
    }
}
