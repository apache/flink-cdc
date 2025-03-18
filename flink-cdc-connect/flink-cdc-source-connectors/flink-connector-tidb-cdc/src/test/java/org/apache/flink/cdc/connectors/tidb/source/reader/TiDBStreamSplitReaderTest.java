package org.apache.flink.cdc.connectors.tidb.source.reader;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetFactory;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TiDBStreamSplitReaderTest extends TiDBTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBStreamSplitReaderTest.class);
    private static final String databaseName = "customer";
    private static final String tableName = "customers";
    private static final String STREAM_SPLIT_ID = "stream-split";

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;
    private static final int MAX_RETRY_TIMES = 100;

    private TiDBSourceConfig sourceConfig;
    private TiDBDialect tiDBDialect;
    private EventOffsetFactory cdcEventOffsetFactory;

    @Before
    public void before() {
        initializeTidbTable("customer");
        TiDBSourceConfigFactory tiDBSourceConfigFactory = new TiDBSourceConfigFactory();
        tiDBSourceConfigFactory.pdAddresses(
                PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN));
        tiDBSourceConfigFactory.hostname(TIDB.getHost());
        tiDBSourceConfigFactory.port(TIDB.getMappedPort(TIDB_PORT));
        tiDBSourceConfigFactory.username(TiDBTestBase.TIDB_USER);
        tiDBSourceConfigFactory.password(TiDBTestBase.TIDB_PASSWORD);
        tiDBSourceConfigFactory.databaseList(this.databaseName);
        tiDBSourceConfigFactory.tableList(this.databaseName + "." + this.tableName);
        tiDBSourceConfigFactory.splitSize(10);
        tiDBSourceConfigFactory.skipSnapshotBackfill(true);
        tiDBSourceConfigFactory.scanNewlyAddedTableEnabled(true);
        this.sourceConfig = tiDBSourceConfigFactory.create(0);
        this.tiDBDialect = new TiDBDialect(tiDBSourceConfigFactory.create(0));
        this.cdcEventOffsetFactory = new EventOffsetFactory();
    }

    @Test
    public void testStreamSplitReader() throws Exception {
        String tableId = databaseName + "." + tableName;
        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(new TestingReaderContext());
        IncrementalSourceSplitReader<JdbcSourceConfig> streamSplitReader =
                new IncrementalSourceSplitReader<>(
                        0,
                        tiDBDialect,
                        sourceConfig,
                        incrementalSourceReaderContext,
                        SnapshotPhaseHooks.empty());
        try {
            EventOffset startOffset = new EventOffset(Instant.now().toEpochMilli());
            String[] insertDataSql =
                    new String[]{
                            "INSERT INTO "
                                    + tableId
                                    + " VALUES(112, 'user_12','Shanghai','123567891234')",
                            "INSERT INTO "
                                    + tableId
                                    + " VALUES(113, 'user_13','Shanghai','123567891234')",
                    };
            try (TiDBConnection tiDBConnection = tiDBDialect.openJdbcConnection()) {
                tiDBConnection.execute(insertDataSql);
                tiDBConnection.commit();
            }
            TableId tableIds = new TableId(databaseName, null, tableName);
            Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
            tableSchemas.put(tableIds, null);
            FinishedSnapshotSplitInfo finishedSnapshotSplitInfo =
                    new FinishedSnapshotSplitInfo(
                            tableIds,
                            STREAM_SPLIT_ID,
                            new Object[]{startOffset},
                            new Object[]{EventOffset.NO_STOPPING_OFFSET},
                            startOffset,
                            cdcEventOffsetFactory);
            StreamSplit streamSplit =
                    new StreamSplit(
                            STREAM_SPLIT_ID,
                            startOffset,
                            cdcEventOffsetFactory.createNoStoppingOffset(),
                            Collections.singletonList(finishedSnapshotSplitInfo),
                            tableSchemas,
                            0);
            assertTrue(streamSplitReader.canAssignNextSplit());
            streamSplitReader.handleSplitsChanges(new SplitsAddition<>(singletonList(streamSplit)));
            int retry = 0;
            int count = 0;
            while (retry < MAX_RETRY_TIMES) {
                ChangeEventRecords records = (ChangeEventRecords) streamSplitReader.fetch();
                if (records.nextSplit() != null) {
                    SourceRecords sourceRecords;
                    while ((sourceRecords = records.nextRecordFromSplit()) != null) {
                        Iterator<SourceRecord> iterator = sourceRecords.iterator();
                        while (iterator.hasNext()) {
                            Struct value = (Struct) iterator.next().value();
                            String opType = value.getString("op");
                            assertEquals(opType, "c");
                            Struct after = (Struct) value.get("after");
                            String name = after.getString("name");

                            assertTrue(name.contains("user"));
                            if (++count >= insertDataSql.length) {
                                return;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            LOG.error("Stream split read error.", e);
        } finally {
            streamSplitReader.close();
        }
    }
}
