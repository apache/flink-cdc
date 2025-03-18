package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.relational.TableId;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.testutils.RecordsFormatter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TiDBScanFetchTask}.
 */

/**
 * VALUES (default, "scooter", "Small 2-wheel scooter", 3.14), (default, "car battery", "12V car
 * battery", 8.1), (default, "12-pack drill bits", "12-pack of drill bits with sizes ranging from
 * #40 to #3", 0.8), (default, "hammer", "12oz carpenter's hammer", 0.75), (default, "hammer", "14oz
 * carpenter's hammer", 0.875), (default, "hammer", "16oz carpenter's hammer", 1.0), (default,
 * "rocks", "box of assorted rocks", 5.3), (default, "jacket", "water resistent black wind breaker",
 * 0.1), (default, "spare tire", "24 inch spare tire", 22.2);
 */
public class TiDBScanFetchTaskTest extends TiDBTestBase {
    private static final String databaseName = "customer";
    private static final String tableName = "customers";

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    @Test
    public void testChangingDataInSnapshotScan() throws Exception {
        initializeTidbTable("customer");
        String tableId = databaseName + "." + tableName;
        String[] changingDataSql =
                new String[]{
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                        "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
                };
        String[] expected =
                new String[]{
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
                getDataInSnapshotScan(
                        changingDataSql,
                        USE_POST_LOWWATERMARK_HOOK,
                        false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testInsertDataInSnapshotScan() throws Exception {
        initializeTidbTable("customer");
        String tableId = databaseName + "." + tableName;
        String[] insertDataSql =
                new String[]{
                        "INSERT INTO " + tableId + " VALUES(112, 'user_12','Shanghai','123567891234')",
                        "INSERT INTO " + tableId + " VALUES(113, 'user_13','Shanghai','123567891234')",
                };

        String[] expected =
                new String[]{
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
                        insertDataSql, USE_POST_LOWWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testDeleteDataInSnapshotScan() throws Exception {
        initializeTidbTable("customer");
        String tableId = databaseName + "." + tableName;
        String[] deleteDataSql =
                new String[]{
                        "DELETE FROM " + tableId + " where id = 101",
                        "DELETE FROM " + tableId + " where id = 102",
                };
        String[] expected =
                new String[]{
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
                        deleteDataSql, USE_POST_LOWWATERMARK_HOOK, false);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testSnapshotScanSkipBackfillWithPostLowWatermark() throws Exception {
        initializeTidbTable("customer");
        String tableId = databaseName + "." + tableName;

        String[] changingDataSql =
                new String[]{
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                        "DELETE FROM " + tableId + " where id = 102",
                        "INSERT INTO " + tableId + " VALUES(102, 'user_2','hangzhou','123567891234')",
                        "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
                };

        String[] expected =
                new String[]{
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
                        changingDataSql, USE_POST_LOWWATERMARK_HOOK, true);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    @Test
    public void testSnapshotScanSkipBackfillWithPreHighWatermark() throws Exception {
        initializeTidbTable("customer");
        String tableId = databaseName + "." + tableName;

        String[] changingDataSql =
                new String[]{
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                        //                    "DELETE FROM " + tableId + " where id = 102",
                        //                    "INSERT INTO " + tableId + " VALUES(102,
                        // 'user_2',Hangzhou','123567891234')",
                        "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                        "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
                };

        String[] expected =
                new String[]{
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

        // Change data during [snapshot, high_watermark) will not be captured by snapshotting
        List<String> actual =
                getDataInSnapshotScan(
                        changingDataSql, USE_POST_LOWWATERMARK_HOOK, true);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    private List<String> getDataInSnapshotScan(
            String[] changingDataSql,
            int hookType,
            boolean skipSnapshotBackfill)
            throws Exception {
        TiDBSourceConfigFactory tiDBSourceConfigFactory = new TiDBSourceConfigFactory();
        tiDBSourceConfigFactory.hostname(TIDB.getHost());
        tiDBSourceConfigFactory.port(TIDB.getMappedPort(TIDB_PORT));
        tiDBSourceConfigFactory.username(TiDBTestBase.TIDB_USER);
        tiDBSourceConfigFactory.password(TiDBTestBase.TIDB_PASSWORD);
        tiDBSourceConfigFactory.databaseList(this.databaseName);
        tiDBSourceConfigFactory.tableList(this.databaseName + "." + this.tableName);
        tiDBSourceConfigFactory.splitSize(10);
        tiDBSourceConfigFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        TiDBSourceConfig tiDBSourceConfig = tiDBSourceConfigFactory.create(0);
        TiDBDialect tiDBDialect = new TiDBDialect(tiDBSourceConfigFactory.create(0));
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();

        try (TiDBConnection tiDBConnection = tiDBDialect.openJdbcConnection()) {
            SnapshotPhaseHook snapshotPhaseHook =
                    (tidbSourceConfig, split) -> {
                        tiDBConnection.execute(changingDataSql);
                        tiDBConnection.commit();
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
            List<SnapshotSplit> snapshotSplits = getSnapshotSplits(tiDBSourceConfig, tiDBDialect);

            TiDBSourceFetchTaskContext tidbsourceFetchTaskContext =
                    new TiDBSourceFetchTaskContext(tiDBSourceConfig, tiDBDialect, tiDBConnection);

            return readTableSnapshotSplits(
                    snapshotSplits, tidbsourceFetchTaskContext, 1, dataType, hooks);
        }
    }

    private List<String> readTableSnapshotSplits(
            List<SnapshotSplit> snapshotSplits,
            TiDBSourceFetchTaskContext taskContext,
            int scanSplitsNum,
            DataType dataType,
            SnapshotPhaseHooks snapshotPhaseHooks)
            throws Exception {
        IncrementalSourceScanFetcher sourceScanFetcher =
                new IncrementalSourceScanFetcher(taskContext, 0);

        ArrayList<SourceRecord> result = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            SnapshotSplit sqlSplit = snapshotSplits.get(i);
            if (sourceScanFetcher.isFinished()) {
                FetchTask<SourceSplitBase> fetchTask =
                        taskContext
                                .getDataSourceDialect()
                                .createFetchTask(sqlSplit);
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

    /**
     * 于从给定的 TiDB 配置和 JDBC 数据源方言生成SnapshotSplit列表
     */
    private List<SnapshotSplit> getSnapshotSplits(
            TiDBSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect) throws Exception {
        List<TableId> discoverTables = sourceDialect.discoverDataCollections(sourceConfig);
        final ChunkSplitter chunkSplitter = sourceDialect.createChunkSplitter(sourceConfig);
        chunkSplitter.open();

        List<SnapshotSplit> snapshotSplitList = new ArrayList<>();
        for (TableId table : discoverTables) {
            Collection<SnapshotSplit> snapshotSplits = chunkSplitter.generateSplits(table);
            snapshotSplitList.addAll(snapshotSplits);
        }
        return snapshotSplitList;
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

    protected TiDBSourceConfigFactory getMockTiDBSourceConfigFactory(
            String hostName,
            int port,
            String userName,
            String password,
            String databaseName,
            String schemaName,
            String tableName,
            int splitSize,
            boolean skipSnapshotBackfill) {

        TiDBSourceConfigFactory tiDBSourceConfigFactory = new TiDBSourceConfigFactory();
        tiDBSourceConfigFactory.hostname(hostName);
        tiDBSourceConfigFactory.port(port);
        tiDBSourceConfigFactory.username(userName);
        tiDBSourceConfigFactory.password(password);
        tiDBSourceConfigFactory.databaseList(databaseName);
        tiDBSourceConfigFactory.tableList(schemaName + "." + tableName);
        tiDBSourceConfigFactory.splitSize(splitSize);
        tiDBSourceConfigFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return tiDBSourceConfigFactory;
    }
}
