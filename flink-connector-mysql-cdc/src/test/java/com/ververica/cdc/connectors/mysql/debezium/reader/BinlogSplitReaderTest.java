/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.testutils.RecordsFormatter;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getSnapshotSplitInfo;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getStartingOffsetOfBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isHighWatermarkEvent;

/** Tests for {@link BinlogSplitReader}. */
public class BinlogSplitReaderTest extends MySqlParallelSourceTestBase {

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private BinaryLogClient binaryLogClient;
    private MySqlConnection mySqlConnection;

    @Test
    public void testReadSingleBinlogSplit() throws Exception {
        customerDatabase.createAndInitialize();
        Configuration configuration = getConfig(new String[] {"customers"});
        binaryLogClient = StatefulTaskContext.getBinaryClient(configuration);
        mySqlConnection = StatefulTaskContext.getConnection(configuration);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<MySqlSnapshotSplit> splits = getMySqlSplits(configuration);
        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]"
                };
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        pkType,
                        configuration,
                        1,
                        expected.length,
                        splits.get(splits.size() - 1).getTableId());
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadAllBinlogSplitsForOneTable() throws Exception {
        customerDatabase.createAndInitialize();
        Configuration configuration = getConfig(new String[] {"customers"});
        binaryLogClient = StatefulTaskContext.getBinaryClient(configuration);
        mySqlConnection = StatefulTaskContext.getConnection(configuration);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<MySqlSnapshotSplit> splits = getMySqlSplits(configuration);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, Hangzhou, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        pkType,
                        configuration,
                        splits.size(),
                        expected.length,
                        splits.get(splits.size() - 1).getTableId());
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadAllBinlogForTableWithSingleLine() throws Exception {
        customerDatabase.createAndInitialize();
        Configuration configuration = getConfig(new String[] {"customer_card_single_line"});
        binaryLogClient = StatefulTaskContext.getBinaryClient(configuration);
        mySqlConnection = StatefulTaskContext.getConnection(configuration);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                        DataTypes.FIELD("level", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("note", DataTypes.STRING()));
        List<MySqlSnapshotSplit> splits = getMySqlSplits(configuration);

        String[] expected =
                new String[] {
                    "+I[20000, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_2, user_2, user with level 2]",
                    "+I[20002, LEVEL_3, user_3, user with level 3]"
                };

        final RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                                        DataTypes.FIELD("level", DataTypes.STRING()))
                                .getLogicalType();
        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        pkType,
                        configuration,
                        splits.size(),
                        expected.length,
                        splits.get(splits.size() - 1).getTableId());
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadAllBinlogSplitsForTables() throws Exception {
        customerDatabase.createAndInitialize();
        Configuration configuration =
                getConfig(new String[] {"customer_card", "customer_card_single_line"});
        binaryLogClient = StatefulTaskContext.getBinaryClient(configuration);
        mySqlConnection = StatefulTaskContext.getConnection(configuration);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                        DataTypes.FIELD("level", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("note", DataTypes.STRING()));
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                                        DataTypes.FIELD("level", DataTypes.STRING()))
                                .getLogicalType();
        List<MySqlSnapshotSplit> splits = getMySqlSplits(configuration);
        String[] expected =
                new String[] {
                    "+I[20000, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_2, user_2, user with level 2]",
                    "+I[20002, LEVEL_3, user_3, user with level 3]",
                    "+I[20001, LEVEL_4, user_1, user with level 4]",
                    "+I[20002, LEVEL_4, user_2, user with level 4]",
                    "+I[20003, LEVEL_4, user_3, user with level 4]",
                    "+I[20004, LEVEL_1, user_4, user with level 4]",
                    "+I[20004, LEVEL_2, user_4, user with level 4]",
                    "+I[20004, LEVEL_3, user_4, user with level 4]",
                    "+I[20004, LEVEL_4, user_4, user with level 4]",
                    "+I[30006, LEVEL_3, user_5, user with level 3]",
                    "+I[30007, LEVEL_3, user_6, user with level 3]",
                    "+I[30008, LEVEL_3, user_7, user with level 3]",
                    "+I[30009, LEVEL_1, user_8, user with level 3]",
                    "+I[30009, LEVEL_2, user_8, user with level 3]",
                    "+I[30009, LEVEL_3, user_8, user with level 3]",
                    "+I[40001, LEVEL_2, user_9, user with level 2]",
                    "+I[40002, LEVEL_2, user_10, user with level 2]",
                    "+I[40003, LEVEL_2, user_11, user with level 2]",
                    "+I[50001, LEVEL_1, user_12, user with level 1]",
                    "+I[50002, LEVEL_1, user_13, user with level 1]",
                    "+I[50003, LEVEL_1, user_14, user with level 1]"
                };
        List<String> actual =
                readBinlogSplits(
                        splits,
                        dataType,
                        pkType,
                        configuration,
                        splits.size(),
                        expected.length,
                        // make the result deterministic
                        TableId.parse(
                                customerDatabase.getDatabaseName()
                                        + "."
                                        + "customer_card_single_line"));
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testReadBinlogFromLatestOffset() throws Exception {
        customerDatabase.createAndInitialize();
        Configuration configuration = getConfig(new String[] {"customers"});
        configuration.set(SCAN_STARTUP_MODE, "latest-offset");
        binaryLogClient = StatefulTaskContext.getBinaryClient(configuration);
        mySqlConnection = StatefulTaskContext.getConnection(configuration);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        String[] expected =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, Hangzhou, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> actual =
                readBinlogSplitsFromLatestOffset(dataType, configuration, expected.length);
        assertEqualsInOrder(Arrays.asList(expected), actual);
    }

    private List<String> readBinlogSplitsFromLatestOffset(
            DataType dataType, Configuration configuration, int expectedSize) throws Exception {
        final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(configuration, binaryLogClient, mySqlConnection);

        // step-1: create binlog split
        MySqlBinlogSplitAssigner binlogSplitAssigner = new MySqlBinlogSplitAssigner(configuration);
        binlogSplitAssigner.open();
        MySqlSplit binlogSplit = binlogSplitAssigner.getNext().get();

        // step-2: test read binlog split
        BinlogSplitReader binlogReader = new BinlogSplitReader(statefulTaskContext, 0);
        binlogReader.submitSplit(binlogSplit);

        // step-3: make some binlog events
        TableId tableId = binlogSplit.getTableSchemas().keySet().iterator().next();

        if (tableId.table().contains("customers")) {
            makeCustomersBinlogEvents(
                    statefulTaskContext.getConnection(), tableId.toString(), false);
        } else {
            makeCustomerCardsBinlogEvents(statefulTaskContext.getConnection(), tableId.toString());
        }

        // step-4: fetched all produced binlog data and format them
        List<String> actual = new ArrayList<>();
        Iterator<SourceRecord> recordIterator;
        List<SourceRecord> fetchedRecords = new ArrayList<>();
        while ((recordIterator = binlogReader.pollSplitRecords()) != null) {
            while (recordIterator.hasNext()) {
                fetchedRecords.add(recordIterator.next());
            }
            actual.addAll(formatResult(fetchedRecords, dataType));
            fetchedRecords.clear();
            if (actual.size() >= expectedSize) {
                break;
            }
        }
        return actual;
    }

    private List<String> readBinlogSplits(
            List<MySqlSnapshotSplit> sqlSplits,
            DataType dataType,
            RowType pkType,
            Configuration configuration,
            int scanSplitsNum,
            int expectedSize,
            TableId binlogChangeTableId)
            throws Exception {
        final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(configuration, binaryLogClient, mySqlConnection);
        final SnapshotSplitReader snapshotSplitReader =
                new SnapshotSplitReader(statefulTaskContext, 0);

        // step-1: read snapshot splits firstly
        List<SourceRecord> fetchedRecords = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            MySqlSplit sqlSplit = sqlSplits.get(i);
            if (snapshotSplitReader.isFinished()) {
                snapshotSplitReader.submitSplit(sqlSplit);
            }
            Iterator<SourceRecord> res;
            while ((res = snapshotSplitReader.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    SourceRecord sourceRecord = res.next();
                    fetchedRecords.add(sourceRecord);
                }
            }
        }

        // step-2: create binlog split according the finished snapshot splits
        List<FinishedSnapshotSplitInfo> finishedSplitsInfo =
                getFinishedSplitsInfo(sqlSplits, fetchedRecords);
        BinlogOffset startingOffset = getStartingOffsetOfBinlogSplit(finishedSplitsInfo);
        Map<TableId, TableChange> tableSchemas = new HashMap<>();
        for (MySqlSplit mySqlSplit : sqlSplits) {
            tableSchemas.putAll(mySqlSplit.getTableSchemas());
        }
        MySqlSplit binlogSplit =
                new MySqlBinlogSplit(
                        "binlog-split",
                        pkType,
                        startingOffset,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        finishedSplitsInfo,
                        tableSchemas);

        // step-3: test read binlog split
        BinlogSplitReader binlogReader = new BinlogSplitReader(statefulTaskContext, 0);
        binlogReader.submitSplit(binlogSplit);

        // step-4: make some binlog events
        if (binlogChangeTableId.table().contains("customers")) {
            makeCustomersBinlogEvents(
                    statefulTaskContext.getConnection(),
                    binlogChangeTableId.toString(),
                    scanSplitsNum == 1);
        } else {
            makeCustomerCardsBinlogEvents(
                    statefulTaskContext.getConnection(), binlogChangeTableId.toString());
        }

        // step-5: fetched all produced binlog data and format them
        List<String> actual = new ArrayList<>();
        Iterator<SourceRecord> recordIterator;
        while ((recordIterator = binlogReader.pollSplitRecords()) != null) {
            while (recordIterator.hasNext()) {
                fetchedRecords.add(recordIterator.next());
            }
            actual.addAll(formatResult(fetchedRecords, dataType));
            fetchedRecords.clear();
            if (actual.size() >= expectedSize) {
                break;
            }
        }
        return actual;
    }

    private void makeCustomersBinlogEvents(
            JdbcConnection connection, String tableId, boolean firstSplitOnly) throws SQLException {
        // make binlog events for the first split
        connection.setAutoCommit(false);
        connection.execute(
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                "DELETE FROM " + tableId + " where id = 102",
                "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
        connection.commit();

        if (!firstSplitOnly) {
            // make binlog events for split-1
            connection.execute("UPDATE " + tableId + " SET name = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','Shanghai','123567891234')");
            connection.commit();

            // make schema change binlog events
            connection.execute(
                    "ALTER TABLE "
                            + tableId
                            + " ADD COLUMN email VARCHAR(128) DEFAULT 'user@flink.apache.org'");
            connection.commit();

            // make binlog events after schema changed
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2002, 'user_23','Shanghai','123567891234', 'test1@gmail.com')");
            connection.commit();

            // make binlog again
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2003, 'user_24','Shanghai','123567891234', 'test2@gmail.com')");
            connection.commit();
        }
    }

    private void makeCustomerCardsBinlogEvents(JdbcConnection connection, String tableId)
            throws SQLException {
        if (tableId.endsWith("customer_card_single_line")) {
            // make binlog events for the first split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20000, 'LEVEL_1', 'user_1', 'user with level 1')");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20001, 'LEVEL_2', 'user_2', 'user with level 2')",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20002, 'LEVEL_3', 'user_3', 'user with level 3')");

            connection.commit();
        } else {
            // make binlog events for the first split
            connection.execute(
                    "UPDATE " + tableId + " SET level = 'LEVEL_3' where user_id = 'user_1'",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(20002, 'LEVEL_5', 'user_15', 'user with level 15'");
            connection.commit();

            // make binlog events for middle split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(40000, 'LEVEL_1', 'user_16', 'user with level 1')",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(40004, 'LEVEL_2', 'user_17', 'user with level 2')");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(50004, 'LEVEL_1', 'user_18', 'user with level 1')",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(50005, 'LEVEL_2', 'user_19', 'user with level 2')");
            connection.commit();
        }
    }

    private List<FinishedSnapshotSplitInfo> getFinishedSplitsInfo(
            List<MySqlSnapshotSplit> mySqlSplits, List<SourceRecord> records) {
        Map<String, MySqlSnapshotSplit> splitMap = new HashMap<>();
        mySqlSplits.forEach(r -> splitMap.put(r.splitId(), r));

        List<FinishedSnapshotSplitInfo> finishedSplitsInfo = new ArrayList<>();
        records.stream()
                .filter(event -> isHighWatermarkEvent(event))
                .forEach(
                        event -> {
                            Struct value = (Struct) event.value();
                            String splitId = value.getString(SignalEventDispatcher.SPLIT_ID_KEY);
                            MySqlSnapshotSplit mySqlSplit = splitMap.get(splitId);
                            finishedSplitsInfo.add(getSnapshotSplitInfo(mySqlSplit, event));
                        });
        return finishedSplitsInfo;
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<MySqlSnapshotSplit> getMySqlSplits(Configuration configuration) {
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(configuration, DEFAULT_PARALLELISM);
        assigner.open();
        List<MySqlSnapshotSplit> mySqlSplits = new ArrayList<>();
        while (true) {
            Optional<MySqlSplit> mySqlSplit = assigner.getNext();
            if (mySqlSplit.isPresent()) {
                mySqlSplits.add(mySqlSplit.get().asSnapshotSplit());
            } else {
                break;
            }
        }
        assigner.close();
        return mySqlSplits;
    }

    private Configuration getConfig(String[] captureTables) {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.server.name", "embedded-test");
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customerDatabase.getUsername());
        properties.put("database.password", customerDatabase.getPassword());
        properties.put("database.whitelist", customerDatabase.getDatabaseName());
        properties.put("database.history.skip.unparseable.ddl", "true");
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        properties.put("snapshot.mode", "initial");
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.history.instance.name", UUID.randomUUID().toString());
        List<String> captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .collect(Collectors.toList());
        properties.put("table.whitelist", String.join(",", captureTableIds));
        properties.put("scan.incremental.snapshot.chunk.size", "10");
        properties.put("scan.snapshot.fetch.size", "2");

        return Configuration.fromMap(properties);
    }
}
