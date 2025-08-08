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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

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
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresQueryUtils;
import org.apache.flink.cdc.connectors.postgres.testutils.RecordsFormatter;
import org.apache.flink.cdc.connectors.postgres.testutils.TestTableId;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresScanFetchTask}. */
class PostgresScanFetchTaskTest extends PostgresTestBase {
    protected static final int DEFAULT_PARALLELISM = 4;
    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    private static final String schemaName = "customer";
    private static final String tableName = "Customers";

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    void testChangingDataInSnapshotScan() throws Exception {
        customDatabase.createAndInitialize();

        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 103",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId.toSql() + " SET address = 'Shanghai' where \"Id\" = 103",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 110",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 111",
                };

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, hangzhou, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Hangzhou, 123567891234]",
                    "+I[111, user_6, Hangzhou, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };
        List<String> actual =
                getDataInSnapshotScan(
                        changingDataSql, schemaName, tableName, USE_PRE_HIGHWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testInsertDataInSnapshotScan() throws Exception {
        customDatabase.createAndInitialize();
        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] insertDataSql =
                new String[] {
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(112, 'user_12','Shanghai','123567891234')",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(113, 'user_13','Shanghai','123567891234')",
                };
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
                getDataInSnapshotScan(
                        insertDataSql, schemaName, tableName, USE_POST_LOWWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testDeleteDataInSnapshotScan() throws Exception {
        customDatabase.createAndInitialize();
        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] deleteDataSql =
                new String[] {
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 101",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                };
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
                getDataInSnapshotScan(
                        deleteDataSql, schemaName, tableName, USE_PRE_HIGHWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testSnapshotScanSkipBackfillWithPostLowWatermark() throws Exception {
        customDatabase.createAndInitialize();

        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 103",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId.toSql() + " SET address = 'Shanghai' where \"Id\" = 103",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 110",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 111",
                };

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, hangzhou, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Hangzhou, 123567891234]",
                    "+I[111, user_6, Hangzhou, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        // Change data during [low_watermark, snapshot) will not be captured by snapshotting
        List<String> actual =
                getDataInSnapshotScan(
                        changingDataSql, schemaName, tableName, USE_POST_LOWWATERMARK_HOOK, true);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testSnapshotScanSkipBackfillWithPreHighWatermark() throws Exception {
        customDatabase.createAndInitialize();

        TestTableId tableId = new TestTableId(schemaName, tableName);
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 103",
                    "DELETE FROM " + tableId.toSql() + " where \"Id\" = 102",
                    "INSERT INTO "
                            + tableId.toSql()
                            + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId.toSql() + " SET address = 'Shanghai' where \"Id\" = 103",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 110",
                    "UPDATE " + tableId.toSql() + " SET address = 'Hangzhou' where \"Id\" = 111",
                };

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        // Change data during [snapshot, high_watermark) will not be captured by snapshotting
        List<String> actual =
                getDataInSnapshotScan(
                        changingDataSql, schemaName, tableName, USE_PRE_HIGHWATERMARK_HOOK, true);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    void testSnapshotFetchSize() throws Exception {
        customDatabase.createAndInitialize();
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(customDatabase, schemaName, tableName, 10, true);
        Properties properties = new Properties();
        properties.setProperty("snapshot.fetch.size", "2");
        sourceConfigFactory.debeziumProperties(properties);
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfigFactory.create(0));
        SnapshotSplit snapshotSplit = getSnapshotSplits(sourceConfig, postgresDialect).get(0);
        PostgresSourceFetchTaskContext postgresSourceFetchTaskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, postgresDialect);
        final String selectSql =
                PostgresQueryUtils.buildSplitScanQuery(
                        snapshotSplit.getTableId(),
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null,
                        Collections.emptyList());

        try (PostgresConnection jdbcConnection = postgresDialect.openJdbcConnection();
                PreparedStatement selectStatement =
                        PostgresQueryUtils.readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                snapshotSplit.getSplitStart(),
                                snapshotSplit.getSplitEnd(),
                                snapshotSplit.getSplitKeyType().getFieldCount(),
                                postgresSourceFetchTaskContext
                                        .getDbzConnectorConfig()
                                        .getSnapshotFetchSize());
                ResultSet rs = selectStatement.executeQuery()) {
            assertThat(rs.getFetchSize()).isEqualTo(2);
        }
    }

    private List<String> getDataInSnapshotScan(
            String[] changingDataSql,
            String schemaName,
            String tableName,
            int hookType,
            boolean skipSnapshotBackfill)
            throws Exception {
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, schemaName, tableName, 10, skipSnapshotBackfill);
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect postgresDialect = new PostgresDialect(sourceConfigFactory.create(0));
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, postgresDialect);
        try (PostgresConnection postgresConnection = postgresDialect.openJdbcConnection()) {
            SnapshotPhaseHook snapshotPhaseHook =
                    (postgresSourceConfig, split) -> {
                        postgresConnection.execute(changingDataSql);
                        postgresConnection.commit();
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    };

            if (hookType == USE_POST_LOWWATERMARK_HOOK) {
                hooks.setPostLowWatermarkAction(snapshotPhaseHook);
            } else if (hookType == USE_PRE_HIGHWATERMARK_HOOK) {
                hooks.setPreHighWatermarkAction(snapshotPhaseHook);
            }

            final DataType dataType =
                    DataTypes.ROW(
                            DataTypes.FIELD("Id", DataTypes.BIGINT()),
                            DataTypes.FIELD("Name", DataTypes.STRING()),
                            DataTypes.FIELD("address", DataTypes.STRING()),
                            DataTypes.FIELD("phone_number", DataTypes.STRING()));

            PostgresSourceFetchTaskContext postgresSourceFetchTaskContext =
                    new PostgresSourceFetchTaskContext(sourceConfig, postgresDialect);

            return readTableSnapshotSplits(
                    reOrderSnapshotSplits(snapshotSplits),
                    postgresSourceFetchTaskContext,
                    1,
                    dataType,
                    hooks);
        }
    }

    private List<String> readTableSnapshotSplits(
            List<SnapshotSplit> snapshotSplits,
            PostgresSourceFetchTaskContext taskContext,
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

        assertThat(sourceScanFetcher.getExecutorService()).isNotNull();
        assertThat(sourceScanFetcher.getExecutorService().isTerminated()).isTrue();

        return formatResult(result, dataType);
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<SnapshotSplit> getSnapshotSplits(
            PostgresSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect)
            throws Exception {
        List<io.debezium.relational.TableId> discoverTables =
                sourceDialect.discoverDataCollections(sourceConfig);
        OffsetFactory offsetFactory = new PostgresOffsetFactory();
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
