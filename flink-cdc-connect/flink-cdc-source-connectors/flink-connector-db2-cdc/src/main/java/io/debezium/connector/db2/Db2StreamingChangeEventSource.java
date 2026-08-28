/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeTableResultSet;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Copied from Debezium project(2.7.4.Final)..
 *
 * <p>Change 1: the effective offset context is initialised at the top of {@code execute} —
 * Debezium's coordinator calls {@code init(offsetContext)} before {@code execute}, but Flink CDC
 * drives this source directly and never calls {@code init}.
 *
 * <p>Change 2: add the {@code afterHandleLsn} hook, called after each iteration commits its
 * position, so Flink CDC's bounded stream task can stop at the high watermark.
 *
 * <p>Change 3: keep the pre-2.7 exclusive upper bound when deciding which capture instances
 * represent a schema change. Upstream 2.7 widened this to {@code currentMaxLsn.increment()}, making
 * the window inclusive. Flink CDC reads a change table's {@code startLsn} from a column that tracks
 * the capture progress rather than a fixed creation LSN, so {@code startLsn} equals {@code
 * currentMaxLsn} on virtually every poll. With the inclusive bound the same capture instance is
 * queued as a schema change over and over, and {@code migrateTable} re-reads the live source table
 * schema each time. After an {@code ALTER TABLE ... ADD COLUMN} that re-read installs the new,
 * wider schema while the still-open change-table result set only yields the old column count,
 * producing "Data row is smaller than a column index".
 */
public class Db2StreamingChangeEventSource
        implements StreamingChangeEventSource<Db2Partition, Db2OffsetContext> {

    private static final int COL_COMMIT_LSN = 2;
    private static final int COL_ROW_LSN = 3;
    private static final int COL_OPERATION = 1;
    private static final int COL_DATA = 5;

    private static final Pattern MISSING_CDC_FUNCTION_CHANGES_ERROR =
            Pattern.compile("Invalid object name 'cdc.fn_cdc_get_all_changes_(.*)'\\.");

    private static final Logger LOGGER =
            LoggerFactory.getLogger(Db2StreamingChangeEventSource.class);

    /** Connection used for reading CDC tables. */
    private final Db2Connection dataConnection;

    /**
     * A separate connection for retrieving timestamps; without it, adaptive buffering will not
     * work.
     */
    private final Db2Connection metadataConnection;

    private final EventDispatcher<Db2Partition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final Db2DatabaseSchema schema;
    private final Duration pollInterval;
    private final Db2ConnectorConfig connectorConfig;
    private Db2OffsetContext effectiveOffsetContext;

    private final SnapshotterService snapshotterService;

    public Db2StreamingChangeEventSource(
            Db2ConnectorConfig connectorConfig,
            Db2Connection dataConnection,
            Db2Connection metadataConnection,
            EventDispatcher<Db2Partition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            Db2DatabaseSchema schema,
            SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
        this.snapshotterService = snapshotterService;
    }

    public void init(Db2OffsetContext offsetContext) {

        this.effectiveOffsetContext =
                offsetContext != null
                        ? offsetContext
                        : new Db2OffsetContext(connectorConfig, TxLogPosition.NULL, false, false);
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            Db2Partition partition,
            Db2OffsetContext offsetContext)
            throws InterruptedException {
        // Debezium's ChangeEventSourceCoordinator calls init(offsetContext) before execute(...) to
        // set the effective offset context. Flink CDC drives this source directly and never calls
        // init, so fall back to the offset context it passes in.
        if (getOffsetContext() == null) {
            init(offsetContext);
        }

        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        final Queue<Db2ChangeTable> schemaChangeCheckpoints =
                new PriorityQueue<>((x, y) -> x.getStopLsn().compareTo(y.getStopLsn()));
        try {
            final AtomicReference<Db2ChangeTable[]> tablesSlot =
                    new AtomicReference<>(getCdcTablesToQuery(partition, offsetContext));

            final TxLogPosition lastProcessedPositionOnStart = offsetContext.getChangePosition();
            final long lastProcessedEventSerialNoOnStart = offsetContext.getEventSerialNo();
            LOGGER.info(
                    "Last position recorded in offsets is {}[{}]",
                    lastProcessedPositionOnStart,
                    lastProcessedEventSerialNoOnStart);

            TxLogPosition lastProcessedPosition = lastProcessedPositionOnStart;

            // LSN should be increased for the first run only immediately after snapshot completion
            // otherwise we might skip an incomplete transaction after restart
            boolean shouldIncreaseFromLsn = offsetContext.isSnapshotCompleted();
            while (context.isRunning()) {
                final Lsn currentMaxLsn = dataConnection.getMaxLsn();

                // Shouldn't happen if the agent is running, but it is better to guard against such
                // situation
                if (!currentMaxLsn.isAvailable()) {
                    LOGGER.warn(
                            "No maximum LSN in the database; please ensure that the DB2 Agent is running");
                    metronome.pause();
                    continue;
                }
                // There is no change in the database
                if (currentMaxLsn.equals(lastProcessedPosition.getCommitLsn())
                        && shouldIncreaseFromLsn) {
                    LOGGER.debug("No change in the database");
                    metronome.pause();
                    continue;
                }

                // Reading interval is inclusive so we need to move LSN forward but not for first
                // run as TX might not be streamed completely
                final Lsn fromLsn =
                        lastProcessedPosition.getCommitLsn().isAvailable() && shouldIncreaseFromLsn
                                ? dataConnection.incrementLsn(lastProcessedPosition.getCommitLsn())
                                : lastProcessedPosition.getCommitLsn();
                shouldIncreaseFromLsn = true;

                while (!schemaChangeCheckpoints.isEmpty()) {
                    migrateTable(partition, offsetContext, schemaChangeCheckpoints);
                }
                if (!dataConnection.listOfNewChangeTables(fromLsn, currentMaxLsn).isEmpty()) {
                    final Db2ChangeTable[] tables = getCdcTablesToQuery(partition, offsetContext);
                    tablesSlot.set(tables);
                    for (Db2ChangeTable table : tables) {
                        if (table.getStartLsn().isBetween(fromLsn, currentMaxLsn)) {
                            LOGGER.info("Schema will be changed for {}", table);
                            schemaChangeCheckpoints.add(table);
                        }
                    }
                }
                try {
                    dataConnection.getChangesForTables(
                            tablesSlot.get(),
                            fromLsn,
                            currentMaxLsn,
                            resultSets -> {
                                long eventSerialNoInInitialTx = 1;
                                final int tableCount = resultSets.length;
                                final ChangeTablePointer[] changeTables =
                                        new ChangeTablePointer[tableCount];
                                final Db2ChangeTable[] tables = tablesSlot.get();

                                for (int i = 0; i < tableCount; i++) {
                                    changeTables[i] =
                                            new ChangeTablePointer(tables[i], resultSets[i]);
                                    changeTables[i].next();
                                }

                                for (; ; ) {
                                    ChangeTablePointer tableWithSmallestLsn = null;
                                    for (ChangeTablePointer changeTable : changeTables) {
                                        if (changeTable.isCompleted()) {
                                            continue;
                                        }
                                        if (tableWithSmallestLsn == null
                                                || changeTable.compareTo(tableWithSmallestLsn)
                                                        < 0) {
                                            tableWithSmallestLsn = changeTable;
                                        }
                                    }
                                    if (tableWithSmallestLsn == null) {
                                        // No more LSNs available
                                        break;
                                    }

                                    if (!(tableWithSmallestLsn.getChangePosition().isAvailable()
                                            && tableWithSmallestLsn
                                                    .getChangePosition()
                                                    .getInTxLsn()
                                                    .isAvailable())) {
                                        LOGGER.error(
                                                "Skipping change {} as its LSN is NULL which is not expected",
                                                tableWithSmallestLsn);
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }
                                    // After restart for changes that were executed before the last
                                    // committed offset
                                    if (tableWithSmallestLsn
                                                    .getChangePosition()
                                                    .compareTo(lastProcessedPositionOnStart)
                                            < 0) {
                                        LOGGER.info(
                                                "Skipping change {} as its position is smaller than the last recorded position {}",
                                                tableWithSmallestLsn,
                                                lastProcessedPositionOnStart);
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }
                                    // After restart for change that was the last committed and
                                    // operations in it before the last committed offset
                                    if (tableWithSmallestLsn
                                                            .getChangePosition()
                                                            .compareTo(lastProcessedPositionOnStart)
                                                    == 0
                                            && eventSerialNoInInitialTx
                                                    <= lastProcessedEventSerialNoOnStart) {
                                        LOGGER.info(
                                                "Skipping change {} as its order in the transaction {} is smaller than or equal to the last recorded operation {}[{}]",
                                                tableWithSmallestLsn,
                                                eventSerialNoInInitialTx,
                                                lastProcessedPositionOnStart,
                                                lastProcessedEventSerialNoOnStart);
                                        eventSerialNoInInitialTx++;
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }
                                    if (tableWithSmallestLsn
                                                    .getChangeTable()
                                                    .getStopLsn()
                                                    .isAvailable()
                                            && tableWithSmallestLsn
                                                            .getChangeTable()
                                                            .getStopLsn()
                                                            .compareTo(
                                                                    tableWithSmallestLsn
                                                                            .getChangePosition()
                                                                            .getCommitLsn())
                                                    <= 0) {
                                        LOGGER.debug(
                                                "Skipping table change {} as its stop LSN is smaller than the last recorded LSN {}",
                                                tableWithSmallestLsn,
                                                tableWithSmallestLsn.getChangePosition());
                                        tableWithSmallestLsn.next();
                                        continue;
                                    }
                                    LOGGER.trace("Processing change {}", tableWithSmallestLsn);
                                    if (!schemaChangeCheckpoints.isEmpty()) {
                                        if (tableWithSmallestLsn
                                                        .getChangePosition()
                                                        .getCommitLsn()
                                                        .compareTo(
                                                                schemaChangeCheckpoints
                                                                        .peek()
                                                                        .getStopLsn())
                                                >= 0) {
                                            migrateTable(
                                                    partition,
                                                    offsetContext,
                                                    schemaChangeCheckpoints);
                                        }
                                    }
                                    final TableId tableId =
                                            tableWithSmallestLsn
                                                    .getChangeTable()
                                                    .getSourceTableId();
                                    final int operation = tableWithSmallestLsn.getOperation();
                                    final Object[] data = tableWithSmallestLsn.getData();

                                    // UPDATE consists of two consecutive events, first event
                                    // contains
                                    // the row before it was updated and the second the row after
                                    // it was updated
                                    int eventCount = 1;
                                    if (operation == Db2ChangeRecordEmitter.OP_UPDATE_BEFORE) {
                                        if (!tableWithSmallestLsn.next()
                                                || tableWithSmallestLsn.getOperation()
                                                        != Db2ChangeRecordEmitter.OP_UPDATE_AFTER) {
                                            throw new IllegalStateException(
                                                    "The update before event at "
                                                            + tableWithSmallestLsn
                                                                    .getChangePosition()
                                                            + " for table "
                                                            + tableId
                                                            + " was not followed by after event.\n Please report this as a bug together with a events around given LSN.");
                                        }
                                        eventCount = 2;
                                    }
                                    final Object[] dataNext =
                                            (operation == Db2ChangeRecordEmitter.OP_UPDATE_BEFORE)
                                                    ? tableWithSmallestLsn.getData()
                                                    : null;

                                    offsetContext.setChangePosition(
                                            tableWithSmallestLsn.getChangePosition(), eventCount);
                                    offsetContext.event(
                                            tableWithSmallestLsn
                                                    .getChangeTable()
                                                    .getSourceTableId(),
                                            metadataConnection.timestampOfLsn(
                                                    tableWithSmallestLsn
                                                            .getChangePosition()
                                                            .getCommitLsn()));

                                    dispatcher.dispatchDataChangeEvent(
                                            partition,
                                            tableId,
                                            new Db2ChangeRecordEmitter(
                                                    partition,
                                                    offsetContext,
                                                    operation,
                                                    data,
                                                    dataNext,
                                                    clock,
                                                    connectorConfig));
                                    tableWithSmallestLsn.next();
                                }
                            });
                    lastProcessedPosition = TxLogPosition.valueOf(currentMaxLsn);
                    // Terminate the transaction otherwise CDC could not be disabled for tables
                    dataConnection.rollback();
                    // Determine whether to continue streaming in db2 cdc snapshot phase
                    afterHandleLsn(partition, currentMaxLsn);
                } catch (SQLException e) {
                    tablesSlot.set(processErrorFromChangeTableQuery(e, tablesSlot.get()));
                }

                if (context.isPaused()) {
                    LOGGER.info("Streaming will now pause");
                    context.streamingPaused();
                    context.waitSnapshotCompletion();
                    LOGGER.info("Streaming resumed");
                }
            }
        } catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    @Override
    public Db2OffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }

    private void migrateTable(
            Db2Partition partition,
            Db2OffsetContext offsetContext,
            final Queue<Db2ChangeTable> schemaChangeCheckpoints)
            throws InterruptedException, SQLException {
        final Db2ChangeTable newTable = schemaChangeCheckpoints.poll();
        LOGGER.info("Migrating schema to {}", newTable);
        Table tableSchema = metadataConnection.getTableSchemaFromTable(newTable);
        offsetContext.event(newTable.getSourceTableId(), Instant.now());
        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                newTable.getSourceTableId(),
                new Db2SchemaChangeEventEmitter(
                        partition,
                        offsetContext,
                        newTable,
                        tableSchema,
                        schema,
                        SchemaChangeEventType.ALTER));

        newTable.setSourceTable(tableSchema);
    }

    private Db2ChangeTable[] processErrorFromChangeTableQuery(
            SQLException exception, Db2ChangeTable[] currentChangeTables) throws Exception {
        final Matcher m = MISSING_CDC_FUNCTION_CHANGES_ERROR.matcher(exception.getMessage());
        if (m.matches()) {
            final String captureName = m.group(1);
            LOGGER.info("Table is no longer captured with capture instance {}", captureName);
            return Arrays.asList(currentChangeTables).stream()
                    .filter(x -> !x.getCaptureInstance().equals(captureName))
                    .collect(Collectors.toList())
                    .toArray(new Db2ChangeTable[0]);
        }
        throw exception;
    }

    private Db2ChangeTable[] getCdcTablesToQuery(
            Db2Partition partition, Db2OffsetContext offsetContext)
            throws SQLException, InterruptedException {
        final Set<Db2ChangeTable> cdcEnabledTables = dataConnection.listOfChangeTables();

        if (cdcEnabledTables.isEmpty()) {
            LOGGER.warn(
                    "No table has enabled CDC or security constraints prevents getting the list of change tables");
        }

        final Map<TableId, List<Db2ChangeTable>> includedAndCdcEnabledTables =
                cdcEnabledTables.stream()
                        .filter(
                                changeTable -> {
                                    if (connectorConfig
                                            .getTableFilters()
                                            .dataCollectionFilter()
                                            .isIncluded(changeTable.getSourceTableId())) {
                                        return true;
                                    } else {
                                        LOGGER.info(
                                                "CDC is enabled for table {} but the table is not included by connector",
                                                changeTable);
                                        return false;
                                    }
                                })
                        .collect(Collectors.groupingBy(x -> x.getSourceTableId()));

        if (includedAndCdcEnabledTables.isEmpty()) {
            LOGGER.warn(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING);
        }

        final List<Db2ChangeTable> tables = new ArrayList<>();
        for (List<Db2ChangeTable> captures : includedAndCdcEnabledTables.values()) {
            Db2ChangeTable currentTable = captures.get(0);
            if (captures.size() > 1) {
                Db2ChangeTable futureTable;
                if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn()) < 0) {
                    futureTable = captures.get(1);
                } else {
                    currentTable = captures.get(1);
                    futureTable = captures.get(0);
                }
                currentTable.setStopLsn(futureTable.getStartLsn());
                tables.add(futureTable);
                LOGGER.info(
                        "Multiple capture instances present for the same table: {} and {}",
                        currentTable,
                        futureTable);
            }
            if (schema.tableFor(currentTable.getSourceTableId()) == null) {
                LOGGER.info(
                        "Table {} is new to be monitored by capture instance {}",
                        currentTable.getSourceTableId(),
                        currentTable.getCaptureInstance());
                // this prevents potential NPE if there is no TableId information in the source info
                offsetContext.event(currentTable.getSourceTableId(), Instant.now());

                // We need to read the source table schema - nullability information cannot be
                // obtained from change table
                dispatcher.dispatchSchemaChangeEvent(
                        partition,
                        offsetContext,
                        currentTable.getSourceTableId(),
                        new Db2SchemaChangeEventEmitter(
                                partition,
                                offsetContext,
                                currentTable,
                                dataConnection.getTableSchemaFromTable(currentTable),
                                schema,
                                SchemaChangeEventType.CREATE));
            }
            tables.add(currentTable);
        }

        return tables.toArray(new Db2ChangeTable[tables.size()]);
    }

    /**
     * The logical representation of a position for the change in the transaction log. During each
     * sourcing cycle it is necessary to query all change tables and then make a total order of
     * changes across all tables.<br>
     * This class represents an open database cursor over the change table that is able to move the
     * cursor forward and report the LSN for the change to which the cursor now points.
     */
    private static class ChangeTablePointer
            extends ChangeTableResultSet<Db2ChangeTable, TxLogPosition> {

        ChangeTablePointer(Db2ChangeTable changeTable, ResultSet resultSet) {
            super(changeTable, resultSet, COL_DATA);
        }

        @Override
        protected int getOperation(ResultSet resultSet) throws SQLException {
            return resultSet.getInt(COL_OPERATION);
        }

        @Override
        protected TxLogPosition getNextChangePosition(ResultSet resultSet) throws SQLException {
            return isCompleted()
                    ? TxLogPosition.NULL
                    : TxLogPosition.valueOf(
                            Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)),
                            Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
        }
    }

    /** Expose control to the subclass to stop the connector. */
    protected void afterHandleLsn(Db2Partition partition, Lsn toLsn) {
        // do nothing
    }
}
