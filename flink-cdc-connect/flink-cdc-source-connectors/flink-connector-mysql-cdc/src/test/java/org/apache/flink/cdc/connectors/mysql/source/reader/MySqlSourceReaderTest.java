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

package org.apache.flink.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.RecordsFormatter;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.document.Array;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getBinlogPosition;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getFetchTimestamp;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getHistoryRecord;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getMessageTimestamp;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getWatermark;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isHeartbeatEvent;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isHighWatermarkEvent;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isSchemaChangeEvent;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isWatermarkEvent;
import static org.apache.flink.cdc.connectors.mysql.testutils.MetricsUtils.getMySqlSplitEnumeratorContext;
import static org.apache.flink.core.io.InputStatus.END_OF_INPUT;
import static org.apache.flink.core.io.InputStatus.MORE_AVAILABLE;
import static org.apache.flink.util.Preconditions.checkState;

/** Tests for {@link MySqlSourceReader}. */
class MySqlSourceReaderTest extends MySqlSourceTestBase {

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @AfterEach
    public void clear() {
        customerDatabase.dropDatabase();
        inventoryDatabase.dropDatabase();
    }

    @Test
    void testRemoveTableUsingStateFromSnapshotPhase() throws Exception {
        customerDatabase.createAndInitialize();
        MySqlSourceConfig sourceConfig = getConfig(new String[] {"customers", "prefix_customers"});
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<MySqlSplit> snapshotSplits;
        List<MySqlSplit> toRemoveSplits;
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            Map<TableId, TableChanges.TableChange> tableSchemas =
                    TableDiscoveryUtils.discoverSchemaForCapturedTables(
                            new MySqlPartition(
                                    sourceConfig.getMySqlConnectorConfig().getLogicalName()),
                            sourceConfig,
                            jdbc);
            TableId tableId0 = new TableId(customerDatabase.getDatabaseName(), null, "customers");
            TableId tableId1 =
                    new TableId(customerDatabase.getDatabaseName(), null, "prefix_customers");
            RowType splitType =
                    RowType.of(
                            new LogicalType[] {DataTypes.INT().getLogicalType()},
                            new String[] {"id"});
            snapshotSplits =
                    Collections.singletonList(
                            new MySqlSnapshotSplit(
                                    tableId0, 0, splitType, null, null, null, tableSchemas));
            toRemoveSplits =
                    Collections.singletonList(
                            new MySqlSnapshotSplit(
                                    tableId1, 0, splitType, null, null, null, tableSchemas));
        }

        // Step 1: start source reader and assign snapshot splits
        MySqlSourceReader<SourceRecord> reader = createReader(sourceConfig, -1);
        reader.start();
        reader.addSplits(snapshotSplits);

        String[] expectedRecords =
                new String[] {
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        // Step 2: wait the snapshot splits finished reading
        List<String> actualRecords = consumeSnapshotRecords(reader, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expectedRecords), actualRecords);
        reader.handleSourceEvents(
                new FinishedSnapshotSplitsAckEvent(
                        Collections.singletonList(snapshotSplits.get(0).splitId())));

        // Step 3: add splits that need to be removed and do not read it, then snapshot reader's
        // state
        reader.addSplits(toRemoveSplits);
        List<MySqlSplit> splitsState = reader.snapshotState(1L);

        // Step 4: remove table 'prefix_customers' and restart reader from a restored state
        sourceConfig = getConfig(new String[] {"customers"});
        TestingReaderContext readerContext = new TestingReaderContext();
        MySqlSourceReader<SourceRecord> restartReader =
                createReader(sourceConfig, readerContext, -1);
        restartReader.start();
        restartReader.addSplits(splitsState);

        // Step 5: check the finished unacked splits between original reader and restarted reader
        Assertions.assertThat(reader.getFinishedUnackedSplits()).isEmpty();
        // one from the start method and one from the addSplits method
        Assertions.assertThat(readerContext.getNumSplitRequests()).isEqualTo(2);

        reader.close();
        restartReader.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFinishedUnackedSplitsUsingStateFromSnapshotPhase(boolean skipBackFill)
            throws Exception {
        customerDatabase.createAndInitialize();
        final MySqlSourceConfig sourceConfig = getConfig(new String[] {"customers"}, skipBackFill);
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<MySqlSplit> snapshotSplits;
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            Map<TableId, TableChanges.TableChange> tableSchemas =
                    TableDiscoveryUtils.discoverSchemaForCapturedTables(
                            new MySqlPartition(
                                    sourceConfig.getMySqlConnectorConfig().getLogicalName()),
                            sourceConfig,
                            jdbc);
            TableId tableId = new TableId(customerDatabase.getDatabaseName(), null, "customers");
            RowType splitType =
                    RowType.of(
                            new LogicalType[] {DataTypes.INT().getLogicalType()},
                            new String[] {"id"});
            snapshotSplits =
                    Arrays.asList(
                            new MySqlSnapshotSplit(
                                    tableId,
                                    0,
                                    splitType,
                                    null,
                                    new Integer[] {200},
                                    null,
                                    tableSchemas),
                            new MySqlSnapshotSplit(
                                    tableId,
                                    1,
                                    splitType,
                                    new Integer[] {200},
                                    new Integer[] {1500},
                                    null,
                                    tableSchemas),
                            new MySqlSnapshotSplit(
                                    tableId,
                                    2,
                                    splitType,
                                    new Integer[] {1500},
                                    null,
                                    null,
                                    tableSchemas));
        }

        // Step 1: start source reader and assign snapshot splits
        MySqlSourceReader<SourceRecord> reader = createReader(sourceConfig, -1);
        reader.start();
        reader.addSplits(snapshotSplits);

        String[] expectedRecords =
                new String[] {
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        // Step 2: wait the snapshot splits finished reading
        List<String> actualRecords = consumeSnapshotRecords(reader, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expectedRecords), actualRecords);

        // Step 3: snapshot reader's state
        List<MySqlSplit> splitsState = reader.snapshotState(1L);

        // Step 4: restart reader from a restored state
        MySqlSourceReader<SourceRecord> restartReader = createReader(sourceConfig, -1);
        restartReader.start();
        restartReader.addSplits(splitsState);

        // Step 5: check the finished unacked splits between original reader and restarted reader
        Assertions.assertThat(reader.getFinishedUnackedSplits()).hasSize(3);
        assertMapEquals(
                restartReader.getFinishedUnackedSplits(), reader.getFinishedUnackedSplits());
        reader.close();
        restartReader.close();
    }

    @Test
    void testBinlogReadFailoverCrossTransaction() throws Exception {
        customerDatabase.createAndInitialize();
        final MySqlSourceConfig sourceConfig = getConfig(new String[] {"customers"});
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        MySqlSplit binlogSplit;
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            Map<TableId, TableChanges.TableChange> tableSchemas =
                    TableDiscoveryUtils.discoverSchemaForCapturedTables(
                            new MySqlPartition(
                                    sourceConfig.getMySqlConnectorConfig().getLogicalName()),
                            sourceConfig,
                            jdbc);
            binlogSplit =
                    MySqlBinlogSplit.fillTableSchemas(
                            createBinlogSplit(sourceConfig).asBinlogSplit(), tableSchemas);
        }

        MySqlSourceReader<SourceRecord> reader = createReader(sourceConfig, 1);
        reader.start();
        reader.addSplits(Collections.singletonList(binlogSplit));

        // step-1: make 6 change events in one MySQL transaction
        TableId tableId = binlogSplit.getTableSchemas().keySet().iterator().next();
        makeBinlogEventsInOneTransaction(sourceConfig, tableId.toString());

        // step-2: fetch the first 2 records belong to the MySQL transaction
        String[] expectedRecords =
                new String[] {
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]"
                };
        // the 2 records are produced by 1 operations
        List<String> actualRecords = consumeBinlogRecords(reader, dataType);
        assertEqualsInOrder(Arrays.asList(expectedRecords), actualRecords);
        List<MySqlSplit> splitsState = reader.snapshotState(1L);
        // check the binlog split state
        Assertions.assertThat(splitsState).hasSize(1);
        reader.close();

        // step-3: mock failover from a restored state
        MySqlSourceReader<SourceRecord> restartReader = createReader(sourceConfig, 3);
        restartReader.start();
        restartReader.addSplits(splitsState);

        // step-4: fetch the rest 4 records belong to the MySQL transaction
        String[] expectedRestRecords =
                new String[] {
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]"
                };
        // the 4 records are produced by 3 operations
        List<String> restRecords = consumeBinlogRecords(restartReader, dataType);
        assertEqualsInOrder(Arrays.asList(expectedRestRecords), restRecords);
        restartReader.close();
    }

    @Test
    void testRemoveSplitAccordingToNewFilter() throws Exception {
        inventoryDatabase.createAndInitialize();
        List<String> tableNames =
                Arrays.asList(
                        inventoryDatabase.getDatabaseName() + ".products",
                        inventoryDatabase.getDatabaseName() + ".customers");
        final MySqlSourceConfig sourceConfig =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(getTableNameRegex(tableNames.toArray(new String[0])))
                        .includeSchemaChanges(false)
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .username(customerDatabase.getUsername())
                        .password(customerDatabase.getPassword())
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .createConfig(0);
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        DEFAULT_PARALLELISM,
                        tableNames.stream().map(TableId::parse).collect(Collectors.toList()),
                        false,
                        getMySqlSplitEnumeratorContext());
        assigner.open();
        List<MySqlSplit> splits = new ArrayList<>();
        MySqlSnapshotSplit split = (MySqlSnapshotSplit) assigner.getNext().get();
        splits.add(split);

        // should contain only one split
        split = (MySqlSnapshotSplit) assigner.getNext().get();
        Assertions.assertThat(assigner.getNext()).isNotPresent();
        splits.add(split);

        // create source config for reader but only with one table
        final MySqlSourceConfig sourceConfig4Reader =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(
                                getTableNameRegex(tableNames.subList(0, 1).toArray(new String[0])))
                        .includeSchemaChanges(false)
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .username(customerDatabase.getUsername())
                        .password(customerDatabase.getPassword())
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .createConfig(0);
        MySqlSourceReader<SourceRecord> reader = createReader(sourceConfig4Reader, 1);
        reader.start();
        reader.addSplits(splits);
        List<MySqlSplit> mySqlSplits = reader.snapshotState(1L);
        Assertions.assertThat(mySqlSplits).hasSize(1);
        reader.close();
    }

    @Test
    void testNoDuplicateRecordsWhenKeepUpdating() throws Exception {
        inventoryDatabase.createAndInitialize();
        String tableName = inventoryDatabase.getDatabaseName() + ".products";
        // use default split size which is large to make sure we only have one snapshot split
        final MySqlSourceConfig sourceConfig =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(tableName)
                        .includeSchemaChanges(false)
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .username(customerDatabase.getUsername())
                        .password(customerDatabase.getPassword())
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .createConfig(0);
        final MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        DEFAULT_PARALLELISM,
                        Collections.singletonList(TableId.parse(tableName)),
                        false,
                        getMySqlSplitEnumeratorContext());
        assigner.open();
        MySqlSnapshotSplit snapshotSplit = (MySqlSnapshotSplit) assigner.getNext().get();
        // should contain only one split
        Assertions.assertThat(assigner.getNext()).isNotPresent();
        // and the split is a full range one
        Assertions.assertThat(snapshotSplit.getSplitStart()).isNull();
        Assertions.assertThat(snapshotSplit.getSplitEnd()).isNull();
        assigner.close();

        final AtomicBoolean finishReading = new AtomicBoolean(false);
        final CountDownLatch updatingExecuted = new CountDownLatch(1);
        TestingReaderContext testingReaderContext = new TestingReaderContext();
        MySqlSourceReader<SourceRecord> reader =
                createReader(sourceConfig, testingReaderContext, 0, SnapshotPhaseHooks.empty());
        reader.start();

        Thread updateWorker =
                new Thread(
                        () -> {
                            try (Connection connection = inventoryDatabase.getJdbcConnection();
                                    Statement statement = connection.createStatement()) {
                                boolean flagSet = false;
                                while (!finishReading.get()) {
                                    statement.execute(
                                            "UPDATE products SET  description='"
                                                    + UUID.randomUUID().toString()
                                                    + "' WHERE id=101");
                                    if (!flagSet) {
                                        updatingExecuted.countDown();
                                        flagSet = true;
                                    }
                                }
                            } catch (Exception throwables) {
                                throwables.printStackTrace();
                            }
                        });

        // start to keep updating the products table
        updateWorker.start();
        // wait until the updating executed
        updatingExecuted.await();
        // start to read chunks of the products table
        reader.addSplits(Collections.singletonList(snapshotSplit));
        reader.notifyNoMoreSplits();

        TestingReaderOutput<SourceRecord> output = new TestingReaderOutput<>();
        while (true) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                break;
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                reader.isAvailable().get();
            }
        }
        // stop the updating worker
        finishReading.set(true);
        updateWorker.join();

        // check the result
        ArrayList<SourceRecord> emittedRecords = output.getEmittedRecords();
        Map<Object, SourceRecord> recordByKey = new HashMap<>();
        for (SourceRecord record : emittedRecords) {
            SourceRecord existed = recordByKey.get(record.key());
            if (existed != null) {
                Assertions.fail(
                        String.format(
                                "The emitted record contains duplicate records on key\n%s\n%s\n",
                                existed, record));
            } else {
                recordByKey.put(record.key(), record);
            }
        }
        reader.close();
    }

    private MySqlSourceReader<SourceRecord> createReader(MySqlSourceConfig configuration, int limit)
            throws Exception {
        return createReader(
                configuration, new TestingReaderContext(), limit, SnapshotPhaseHooks.empty());
    }

    private MySqlSourceReader<SourceRecord> createReader(
            MySqlSourceConfig configuration, SourceReaderContext readerContext, int limit)
            throws Exception {
        return createReader(configuration, readerContext, limit, SnapshotPhaseHooks.empty());
    }

    private MySqlSourceReader<SourceRecord> createReader(
            MySqlSourceConfig configuration,
            SourceReaderContext readerContext,
            int limit,
            SnapshotPhaseHooks snapshotHooks)
            throws Exception {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        // make  SourceReaderContext#metricGroup compatible between Flink 1.13 and Flink 1.14
        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);
        final MySqlRecordEmitter<SourceRecord> recordEmitter =
                limit > 0
                        ? new MysqlLimitedRecordEmitter(
                                new ForwardDeserializeSchema(),
                                new MySqlSourceReaderMetrics(readerContext.metricGroup()),
                                configuration.isIncludeSchemaChanges(),
                                limit)
                        : new MySqlRecordEmitter<>(
                                new ForwardDeserializeSchema(),
                                new MySqlSourceReaderMetrics(readerContext.metricGroup()),
                                configuration.isIncludeSchemaChanges());
        final MySqlSourceReaderContext mySqlSourceReaderContext =
                new MySqlSourceReaderContext(readerContext);
        return new MySqlSourceReader<>(
                elementsQueue,
                () -> createSplitReader(configuration, mySqlSourceReaderContext, snapshotHooks),
                recordEmitter,
                readerContext.getConfiguration(),
                mySqlSourceReaderContext,
                configuration);
    }

    private MySqlSplitReader createSplitReader(
            MySqlSourceConfig configuration,
            MySqlSourceReaderContext readerContext,
            SnapshotPhaseHooks snapshotHooks) {
        return new MySqlSplitReader(configuration, 0, readerContext, snapshotHooks);
    }

    private void makeBinlogEventsInOneTransaction(MySqlSourceConfig sourceConfig, String tableId)
            throws SQLException {
        JdbcConnection connection = DebeziumUtils.openJdbcConnection(sourceConfig);
        // make 6 binlog events by 4 operations
        connection.setAutoCommit(false);
        connection.execute(
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                "DELETE FROM " + tableId + " where id = 102",
                "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
        connection.commit();
        connection.close();
    }

    private MySqlSplit createBinlogSplit(MySqlSourceConfig sourceConfig) {
        MySqlBinlogSplitAssigner binlogSplitAssigner = new MySqlBinlogSplitAssigner(sourceConfig);
        binlogSplitAssigner.open();
        return binlogSplitAssigner.getNext().get();
    }

    private MySqlSourceConfig getConfig(String[] captureTables) {
        return getConfig(captureTables, false);
    }

    private MySqlSourceConfig getConfig(String[] captureTables, boolean skipBackFill) {
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
                .skipSnapshotBackfill(skipBackFill)
                .createConfig(0);
    }

    private List<String> consumeSnapshotRecords(
            MySqlSourceReader<SourceRecord> sourceReader, DataType recordType) throws Exception {
        // Poll all the  records of the multiple assigned snapshot split.
        sourceReader.notifyNoMoreSplits();
        final SimpleReaderOutput output = new SimpleReaderOutput();
        InputStatus status = MORE_AVAILABLE;
        while (END_OF_INPUT != status) {
            status = sourceReader.pollNext(output);
        }
        final RecordsFormatter formatter = new RecordsFormatter(recordType);
        return formatter.format(output.getResults());
    }

    private List<String> consumeBinlogRecords(
            MySqlSourceReader<SourceRecord> sourceReader, DataType recordType) throws Exception {
        // Poll one batch records of the binlog split.
        final SimpleReaderOutput output = new SimpleReaderOutput();
        while (output.getResults().isEmpty()) {
            sourceReader.pollNext(output);
        }
        final RecordsFormatter formatter = new RecordsFormatter(recordType);
        return formatter.format(output.getResults());
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------
    private static class SimpleReaderOutput implements ReaderOutput<SourceRecord> {

        private final List<SourceRecord> results = new ArrayList<>();

        @Override
        public void collect(SourceRecord record) {
            results.add(record);
        }

        public List<SourceRecord> getResults() {
            return results;
        }

        @Override
        public void collect(SourceRecord record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<SourceRecord> createOutputForSplit(java.lang.String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(java.lang.String splitId) {}
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }

    /**
     * A implementation of {@link RecordEmitter} which only emit records in given limit number, this
     * class is used for test purpose.
     */
    private static class MysqlLimitedRecordEmitter extends MySqlRecordEmitter<SourceRecord> {

        private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordEmitter.class);
        private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
                new FlinkJsonTableChangeSerializer();

        private final DebeziumDeserializationSchema<SourceRecord> debeziumDeserializationSchema;
        private final MySqlSourceReaderMetrics sourceReaderMetrics;
        private final boolean includeSchemaChanges;
        private final OutputCollector<SourceRecord> outputCollector;
        private final int limit;

        public MysqlLimitedRecordEmitter(
                DebeziumDeserializationSchema<SourceRecord> debeziumDeserializationSchema,
                MySqlSourceReaderMetrics sourceReaderMetrics,
                boolean includeSchemaChanges,
                int limit) {
            super(debeziumDeserializationSchema, sourceReaderMetrics, includeSchemaChanges);
            this.debeziumDeserializationSchema = debeziumDeserializationSchema;
            this.sourceReaderMetrics = sourceReaderMetrics;
            this.includeSchemaChanges = includeSchemaChanges;
            this.outputCollector = new OutputCollector<>();
            Preconditions.checkState(limit > 0);
            this.limit = limit;
        }

        @Override
        public void emitRecord(
                SourceRecords sourceRecords,
                SourceOutput<SourceRecord> output,
                MySqlSplitState splitState)
                throws Exception {
            final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
            int sendCnt = 0;
            while (elementIterator.hasNext()) {
                if (sendCnt >= limit) {
                    return;
                }
                processElement(elementIterator.next(), output, splitState);
                sendCnt++;
            }
        }

        protected void processElement(
                SourceRecord element, SourceOutput<SourceRecord> output, MySqlSplitState splitState)
                throws Exception {
            if (isWatermarkEvent(element)) {
                BinlogOffset watermark = getWatermark(element);
                if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                    splitState.asSnapshotSplitState().setHighWatermark(watermark);
                }
            } else if (isSchemaChangeEvent(element) && splitState.isBinlogSplitState()) {
                HistoryRecord historyRecord = getHistoryRecord(element);
                Array tableChanges =
                        historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
                TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
                for (TableChanges.TableChange tableChange : changes) {
                    splitState.asBinlogSplitState().recordSchema(tableChange.getId(), tableChange);
                }
                if (includeSchemaChanges) {
                    BinlogOffset position = getBinlogPosition(element);
                    splitState.asBinlogSplitState().setStartingOffset(position);
                    emitElement(element, output);
                }
            } else if (isDataChangeRecord(element)) {
                updateStartingOffsetForSplit(splitState, element);
                reportMetrics(element);
                emitElement(element, output);
            } else if (isHeartbeatEvent(element)) {
                updateStartingOffsetForSplit(splitState, element);
            } else {
                // unknown element
                LOG.info("Meet unknown element {}, just skip.", element);
            }
        }

        private void updateStartingOffsetForSplit(
                MySqlSplitState splitState, SourceRecord element) {
            if (splitState.isBinlogSplitState()) {
                BinlogOffset position = getBinlogPosition(element);
                splitState.asBinlogSplitState().setStartingOffset(position);
            }
        }

        private void emitElement(SourceRecord element, SourceOutput<SourceRecord> output)
                throws Exception {
            outputCollector.output = output;
            debeziumDeserializationSchema.deserialize(element, outputCollector);
        }

        private void reportMetrics(SourceRecord element) {
            Long messageTimestamp = getMessageTimestamp(element);

            if (messageTimestamp != null && messageTimestamp > 0L) {
                // report fetch delay
                Long fetchTimestamp = getFetchTimestamp(element);
                if (fetchTimestamp != null && fetchTimestamp >= messageTimestamp) {
                    sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
                }
            }
        }

        private static class OutputCollector<T> implements Collector<T> {
            private SourceOutput<T> output;

            @Override
            public void collect(T record) {
                output.collect(record);
            }

            @Override
            public void close() {
                // do nothing
            }
        }
    }

    private String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }
}
