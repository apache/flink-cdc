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

package org.apache.flink.cdc.connectors.oceanbase.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.connection.OceanBaseConnection;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBaseOffsetContext;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBasePartition;
import org.apache.flink.cdc.connectors.oceanbase.source.schema.OceanBaseDatabaseSchema;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseUtils.buildSplitScanQuery;
import static org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseUtils.readTableSplitDataStatement;

/** The task to work for fetching data of OceanBase table snapshot split. */
public class OceanBaseScanFetchTask extends AbstractScanFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseScanFetchTask.class);

    public OceanBaseScanFetchTask(SnapshotSplit snapshotSplit) {
        super(snapshotSplit);
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        OceanBaseSourceFetchTaskContext sourceFetchContext =
                (OceanBaseSourceFetchTaskContext) context;
        OceanBaseSnapshotSplitReadTask snapshotSplitReadTask =
                new OceanBaseSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getEventDispatcher(),
                        snapshotSplit);
        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        SnapshotResult<OceanBaseOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());
        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for OceanBase split %s fail", snapshotResult));
        }
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        // TODO add back fill task
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class OceanBaseSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<OceanBasePartition, OceanBaseOffsetContext> {

        private final OceanBaseConnectorConfig connectorConfig;
        private final OceanBaseDatabaseSchema databaseSchema;
        private final OceanBaseConnection jdbcConnection;
        private final JdbcSourceEventDispatcher<OceanBasePartition> dispatcher;
        private final Clock clock;
        private final SnapshotSplit snapshotSplit;
        private final OceanBaseOffsetContext offsetContext;

        public OceanBaseSnapshotSplitReadTask(
                OceanBaseConnectorConfig connectorConfig,
                OceanBaseOffsetContext previousOffset,
                OceanBaseDatabaseSchema databaseSchema,
                OceanBaseConnection jdbcConnection,
                JdbcSourceEventDispatcher<OceanBasePartition> dispatcher,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, SnapshotProgressListener.NO_OP());
            this.connectorConfig = connectorConfig;
            this.offsetContext = previousOffset;
            this.databaseSchema = databaseSchema;
            this.jdbcConnection = jdbcConnection;
            this.dispatcher = dispatcher;
            this.clock = Clock.SYSTEM;
            this.snapshotSplit = snapshotSplit;
        }

        @Override
        protected SnapshotResult<OceanBaseOffsetContext> doExecute(
                ChangeEventSourceContext context,
                OceanBaseOffsetContext previousOffset,
                SnapshotContext<OceanBasePartition, OceanBaseOffsetContext> snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final OceanBaseSnapshotContext ctx = (OceanBaseSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;
            createDataEvents(ctx, snapshotSplit.getTableId());
            return SnapshotResult.completed(ctx.offset);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                OceanBasePartition partition, OceanBaseOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected SnapshotContext<OceanBasePartition, OceanBaseOffsetContext> prepare(
                OceanBasePartition partition) throws Exception {
            return new OceanBaseSnapshotContext(partition);
        }

        private static class OceanBaseSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        OceanBasePartition, OceanBaseOffsetContext> {

            public OceanBaseSnapshotContext(OceanBasePartition partition) throws SQLException {
                super(partition, "");
            }
        }

        private void createDataEvents(OceanBaseSnapshotContext snapshotContext, TableId tableId)
                throws Exception {
            EventDispatcher.SnapshotReceiver<OceanBasePartition> snapshotReceiver =
                    dispatcher.getSnapshotChangeEventReceiver();
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
            snapshotReceiver.completeSnapshot();
        }

        /** Dispatches the data change events for the records of a single table. */
        private void createDataEventsForTable(
                OceanBaseSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<OceanBasePartition> snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

            final String selectSql =
                    buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.info(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                            readTableSplitDataStatement(
                                    jdbcConnection,
                                    selectSql,
                                    snapshotSplit.getSplitStart() == null,
                                    snapshotSplit.getSplitEnd() == null,
                                    snapshotSplit.getSplitStart(),
                                    snapshotSplit.getSplitEnd(),
                                    snapshotSplit.getSplitKeyType().getFieldCount(),
                                    connectorConfig.getQueryFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row =
                            jdbcConnection.rowToArray(table, databaseSchema, rs, columnArray);
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        logTimer = getTableScanLogTimer();
                    }
                    dispatcher.dispatchSnapshotEvent(
                            snapshotContext.partition,
                            table.id(),
                            getChangeRecordEmitter(snapshotContext, table.id(), row),
                            snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
            }
        }

        protected ChangeRecordEmitter<OceanBasePartition> getChangeRecordEmitter(
                SnapshotContext<OceanBasePartition, OceanBaseOffsetContext> snapshotContext,
                TableId tableId,
                Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter<>(
                    snapshotContext.partition, snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }
    }
}
