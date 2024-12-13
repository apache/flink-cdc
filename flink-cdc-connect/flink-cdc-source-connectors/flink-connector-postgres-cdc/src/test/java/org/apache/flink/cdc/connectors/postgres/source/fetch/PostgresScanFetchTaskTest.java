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
import org.apache.flink.cdc.connectors.postgres.testutils.RecordsFormatter;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link PostgresScanFetchTask}. */
public class PostgresScanFetchTaskTest extends PostgresTestBase {
    protected static final int DEFAULT_PARALLELISM = 4;
    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    private static final String schemaName = "customer";
    private static final String tableName = "customers";

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    public void testChangingDataInSnapshotScan() throws Exception {
        customDatabase.createAndInitialize();

        String tableId = schemaName + "." + tableName;
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
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
    public void testInsertDataInSnapshotScan() throws Exception {
        customDatabase.createAndInitialize();
        String tableId = schemaName + "." + tableName;
        String[] insertDataSql =
                new String[] {
                    "INSERT INTO " + tableId + " VALUES(112, 'user_12','Shanghai','123567891234')",
                    "INSERT INTO " + tableId + " VALUES(113, 'user_13','Shanghai','123567891234')",
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
    public void testDeleteDataInSnapshotScan() throws Exception {
        customDatabase.createAndInitialize();
        String tableId = schemaName + "." + tableName;
        String[] deleteDataSql =
                new String[] {
                    "DELETE FROM " + tableId + " where id = 101",
                    "DELETE FROM " + tableId + " where id = 102",
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
    public void testSnapshotScanSkipBackfillWithPostLowWatermark() throws Exception {
        customDatabase.createAndInitialize();

        String tableId = schemaName + "." + tableName;
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
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
    public void testSnapshotScanSkipBackfillWithPreHighWatermark() throws Exception {
        customDatabase.createAndInitialize();

        String tableId = schemaName + "." + tableName;
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
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
                            DataTypes.FIELD("id", DataTypes.BIGINT()),
                            DataTypes.FIELD("name", DataTypes.STRING()),
                            DataTypes.FIELD("address", DataTypes.STRING()),
                            DataTypes.FIELD("phone_number", DataTypes.STRING()));

            PostgresSourceFetchTaskContext postgresSourceFetchTaskContext =
                    new PostgresSourceFetchTaskContext(sourceConfig, postgresDialect);

            return readTableSnapshotSplits(
                    snapshotSplits, postgresSourceFetchTaskContext, 1, dataType, hooks);
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

        assertNotNull(sourceScanFetcher.getExecutorService());
        assertTrue(sourceScanFetcher.getExecutorService().isTerminated());

        return formatResult(result, dataType);
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<SnapshotSplit> getSnapshotSplits(
            PostgresSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect)
            throws Exception {
        List<TableId> discoverTables = sourceDialect.discoverDataCollections(sourceConfig);
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
}
