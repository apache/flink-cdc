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

package org.apache.flink.cdc.connectors.sqlserver.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceTestBase;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.sqlserver.testutils.RecordsFormatter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase.getConfigFactory;
import static org.apache.flink.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.createSqlServerConnection;

/**
 * Tests for {@link
 * org.apache.flink.cdc.connectors.sqlserver.source.reader.fetch.SqlServerScanFetchTask}.
 */
class SqlServerScanFetchTaskTest extends SqlServerSourceTestBase {

    @Test
    void testChangingDataInSnapshotScan() throws Exception {
        String databaseName = "customer";
        String tableName = "dbo.customers";

        initializeSqlServerTable(databaseName);

        SqlServerSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 10);
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfig);

        String tableId = databaseName + "." + tableName;
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
                };

        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        hooks.setPostLowWatermarkAction(
                (config, split) -> {
                    executeSql((SqlServerSourceConfig) config, changingDataSql);
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
        SqlServerSourceFetchTaskContext sqlServerSourceFetchTaskContext =
                new SqlServerSourceFetchTaskContext(
                        sourceConfig,
                        sqlServerDialect,
                        createSqlServerConnection(sourceConfig.getDbzConnectorConfig()),
                        createSqlServerConnection(sourceConfig.getDbzConnectorConfig()));

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, sqlServerDialect);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Hangzhou, 123567891234]",
                    "+I[111, user_6, Hangzhou, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        List<String> actual =
                readTableSnapshotSplits(
                        reOrderSnapshotSplits(snapshotSplits),
                        sqlServerSourceFetchTaskContext,
                        1,
                        dataType,
                        hooks);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testInsertDataInSnapshotScan() throws Exception {
        String databaseName = "customer";
        String tableName = "dbo.customers";

        initializeSqlServerTable(databaseName);

        SqlServerSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 10);
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfig);

        String tableId = databaseName + "." + tableName;
        String[] insertDataSql =
                new String[] {
                    "INSERT INTO " + tableId + " VALUES(112, 'user_12','Shanghai','123567891234')",
                    "INSERT INTO " + tableId + " VALUES(113, 'user_13','Shanghai','123567891234')",
                };

        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        hooks.setPreHighWatermarkAction(
                (config, split) -> {
                    executeSql((SqlServerSourceConfig) config, insertDataSql);
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
        SqlServerSourceFetchTaskContext sqlServerSourceFetchTaskContext =
                new SqlServerSourceFetchTaskContext(
                        sourceConfig,
                        sqlServerDialect,
                        createSqlServerConnection(sourceConfig.getDbzConnectorConfig()),
                        createSqlServerConnection(sourceConfig.getDbzConnectorConfig()));

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, sqlServerDialect);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[112, user_12, Shanghai, 123567891234]",
                    "+I[113, user_13, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        List<String> actual =
                readTableSnapshotSplits(
                        reOrderSnapshotSplits(snapshotSplits),
                        sqlServerSourceFetchTaskContext,
                        1,
                        dataType,
                        hooks);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testDateTimePrimaryKey() throws Exception {
        String databaseName = "pk";
        String tableName = "dbo.dt_pk";

        initializeSqlServerTable(databaseName);

        SqlServerSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 8096);
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfig);

        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, sqlServerDialect);
        Assertions.assertThat(snapshotSplits).isNotEmpty();

        RowType expectedType =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("dt", DataTypes.TIMESTAMP(3).notNull()))
                                .getLogicalType();

        snapshotSplits.forEach(
                s -> Assertions.assertThat(s.getSplitKeyType()).isEqualTo(expectedType));
    }

    @Test
    void testDeleteDataInSnapshotScan() throws Exception {
        String databaseName = "customer";
        String tableName = "dbo.customers";

        initializeSqlServerTable(databaseName);

        SqlServerSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 10);
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfig);

        String tableId = databaseName + "." + tableName;
        String[] deleteDataSql =
                new String[] {
                    "DELETE FROM " + tableId + " where id = 101",
                    "DELETE FROM " + tableId + " where id = 102",
                };

        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        hooks.setPostLowWatermarkAction(
                (config, split) -> {
                    executeSql((SqlServerSourceConfig) config, deleteDataSql);
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
        SqlServerSourceFetchTaskContext sqlServerSourceFetchTaskContext =
                new SqlServerSourceFetchTaskContext(
                        sourceConfig,
                        sqlServerDialect,
                        createSqlServerConnection(sourceConfig.getDbzConnectorConfig()),
                        createSqlServerConnection(sourceConfig.getDbzConnectorConfig()));

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, sqlServerDialect);

        String[] expected =
                new String[] {
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        List<String> actual =
                readTableSnapshotSplits(
                        reOrderSnapshotSplits(snapshotSplits),
                        sqlServerSourceFetchTaskContext,
                        1,
                        dataType,
                        hooks);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    private List<String> readTableSnapshotSplits(
            List<SnapshotSplit> snapshotSplits,
            SqlServerSourceFetchTaskContext taskContext,
            int scanSplitsNum,
            DataType dataType,
            SnapshotPhaseHooks snapshotPhaseHooks)
            throws Exception {
        IncrementalSourceScanFetcher sourceScanFetcher =
                new IncrementalSourceScanFetcher(taskContext, 0);

        List<SourceRecord> result = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            SnapshotSplit sqlSplit = snapshotSplits.get(i);
            if (sourceScanFetcher.isFinished()) {
                FetchTask<SourceSplitBase> fetchTask =
                        taskContext.getDataSourceDialect().createFetchTask(sqlSplit);
                ((AbstractScanFetchTask) fetchTask).setSnapshotPhaseHooks(snapshotPhaseHooks);
                sourceScanFetcher.submitTask(fetchTask);
            }
            Iterator<SourceRecords> res;
            while ((res = sourceScanFetcher.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    SourceRecords sourceRecords = res.next();
                    result.addAll(sourceRecords.getSourceRecordList());
                }
            }
        }

        sourceScanFetcher.close();

        Assertions.assertThat(sourceScanFetcher.getExecutorService()).isNotNull();
        Assertions.assertThat(sourceScanFetcher.getExecutorService().isTerminated()).isTrue();

        return formatResult(result, dataType);
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<SnapshotSplit> getSnapshotSplits(
            SqlServerSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect)
            throws Exception {
        List<TableId> discoverTables = sourceDialect.discoverDataCollections(sourceConfig);
        OffsetFactory offsetFactory = new LsnFactory();
        final SnapshotSplitAssigner snapshotSplitAssigner =
                new SnapshotSplitAssigner<JdbcSourceConfig>(
                        sourceConfig,
                        DEFAULT_PARALLELISM,
                        discoverTables,
                        sourceDialect.isDataCollectionIdCaseSensitive(sourceConfig),
                        sourceDialect,
                        offsetFactory);
        snapshotSplitAssigner.initEnumeratorMetrics(
                new SourceEnumeratorMetrics(
                        UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup()));
        snapshotSplitAssigner.open();
        List<SnapshotSplit> snapshotSplitList = new ArrayList<>();
        Optional<SourceSplitBase> split = snapshotSplitAssigner.getNext();
        while (split.isPresent()) {
            snapshotSplitList.add(split.get().asSnapshotSplit());
            split = snapshotSplitAssigner.getNext();
        }
        snapshotSplitAssigner.close();
        return snapshotSplitList;
    }

    private boolean executeSql(SqlServerSourceConfig sourceConfig, String[] sqlStatements) {
        try (JdbcConnection connection =
                createSqlServerConnection(sourceConfig.getDbzConnectorConfig())) {
            connection.setAutoCommit(false);
            connection.execute(sqlStatements);
            connection.commit();
        } catch (SQLException e) {
            LOG.error("Failed to execute sql statements.", e);
            return false;
        }
        return true;
    }

    // Due to the default enabling of scan.incremental.snapshot.unbounded-chunk-first.enabled,
    // the split order becomes [end,null], [null,start], ... which is different from the original
    // order.
    // The first split in the list is actually the last unbounded split that should be at the end.
    // This method adjusts the order to restore the original sequence: [null,start], ...,
    // [end,null],
    // ensuring the correctness of test cases.
    private List<SnapshotSplit> reOrderSnapshotSplits(List<SnapshotSplit> snapshotSplits) {
        if (snapshotSplits.size() > 1) {
            SnapshotSplit firstSplit = snapshotSplits.get(0);
            snapshotSplits.remove(0);
            snapshotSplits.add(firstSplit);
        }
        return snapshotSplits;
    }
}
