package com.ververica.cdc.connectors.tdsql.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaEvent;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils;
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;
import com.ververica.cdc.connectors.tdsql.source.events.TdSqlSourceEvent;
import com.ververica.cdc.connectors.tdsql.source.split.TdSqlSplitState;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils.getNextMetaGroupId;

/**
 * The source reader for TdSql source splits.
 *
 * @param <T> The output type for flink.
 */
public class TdSqlSourceReader<T>
        extends SourceReaderBase<SourceRecord, T, TdSqlSplit, TdSqlSplitState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TdSqlSourceReader.class);
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, TdSqlSet> splitIdBelongSet;
    private final Map<String, MySqlBinlogSplit> uncompletedBinlogSplits;
    private final Function<TdSqlSet, MySqlSourceConfig> sourceConfigFunction;

    public TdSqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue,
            SplitFetcherManager<SourceRecord, TdSqlSplit> splitFetcherManager,
            RecordEmitter<SourceRecord, T, TdSqlSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            Function<TdSqlSet, MySqlSourceConfig> sourceConfigFunction) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.finishedUnackedSplits = new HashMap<>();
        this.splitIdBelongSet = new HashMap<>();
        this.uncompletedBinlogSplits = new HashMap<>();
        this.sourceConfigFunction = sourceConfigFunction;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected TdSqlSplitState initializedState(TdSqlSplit split) {
        MySqlSplitState mySqlSplitState;
        if (split.mySqlSplit().isBinlogSplit()) {
            mySqlSplitState = new MySqlBinlogSplitState(split.mySqlSplit().asBinlogSplit());
        } else {
            mySqlSplitState = new MySqlSnapshotSplitState(split.mySqlSplit().asSnapshotSplit());
        }
        return new TdSqlSplitState(split.setInfo(), mySqlSplitState);
    }

    @Override
    public void addSplits(List<TdSqlSplit> splits) {
        List<TdSqlSplit> unfinishedSplits = new ArrayList<>();
        for (TdSqlSplit split : splits) {
            if (split.isBinlogSplit()) {
                context.sendSplitRequest();
                MySqlBinlogSplit binlogSplit = split.mySqlSplit().asBinlogSplit();
                LOGGER.trace(
                        "binlog split {}, isCompletedSplit {}, start offset is {}, getTotalFinishedSplitSize {}",
                        binlogSplit,
                        binlogSplit.isCompletedSplit(),
                        binlogSplit.getStartingOffset(),
                        binlogSplit.getTotalFinishedSplitSize());
                if (!binlogSplit.isCompletedSplit()) {
                    uncompletedBinlogSplits.put(split.splitId(), binlogSplit);
                    requestBinlogSplitMetaIfNeeded(split);
                } else {
                    uncompletedBinlogSplits.remove(split.splitId());
                    split = discoverTableSchemasForBinlogSplit(split);
                    unfinishedSplits.add(split);
                }
            } else {
                MySqlSplit mySqlSplit = split.mySqlSplit();
                if (mySqlSplit.isSnapshotSplit()) {
                    MySqlSnapshotSplit mySqlSnapshotSplit = mySqlSplit.asSnapshotSplit();
                    if (mySqlSnapshotSplit.isSnapshotReadFinished()) {
                        finishedUnackedSplits.put(split.splitId(), mySqlSnapshotSplit);
                    } else {
                        unfinishedSplits.add(split);
                    }
                }
            }
        }
        LOGGER.trace("add splits: {}", JSON.toString(unfinishedSplits));
        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        }
    }

    @Override
    protected TdSqlSplit toSplitType(String splitId, TdSqlSplitState splitState) {
        return new TdSqlSplit(splitState.setInfo(), splitState.mySqlSplit());
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof TdSqlSourceEvent) {
            TdSqlSourceEvent tdSqlSourceEvent = (TdSqlSourceEvent) sourceEvent;
            SourceEvent mySqlSourceEvent = tdSqlSourceEvent.getMySqlEvent();
            TdSqlSet tdSqlSet = tdSqlSourceEvent.getSet();

            if (mySqlSourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
                FinishedSnapshotSplitsAckEvent ackEvent =
                        (FinishedSnapshotSplitsAckEvent) mySqlSourceEvent;
                for (String mySqlSplitId : ackEvent.getFinishedSplits()) {
                    String tdSqlSplitId = tdSqlSet.getSetKey() + ":" + mySqlSplitId;
                    LOGGER.info("remove unack split {}.", tdSqlSplitId);
                    this.finishedUnackedSplits.remove(tdSqlSplitId);
                    this.splitIdBelongSet.remove(tdSqlSplitId);
                }
            } else if (mySqlSourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
                reportFinishedSnapshotSplitsIfNeed();
            } else if (mySqlSourceEvent instanceof BinlogSplitMetaEvent) {
                fillMetaDataForBinlogSplit(
                        tdSqlSourceEvent.getSet(), (BinlogSplitMetaEvent) mySqlSourceEvent);
            }
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    protected void onSplitFinished(Map<String, TdSqlSplitState> finishedSplitIds) {
        for (String splitId : finishedSplitIds.keySet()) {
            LOGGER.info("split reader finish split[{}] read.", splitId);
            TdSqlSplitState state = finishedSplitIds.get(splitId);
            finishedUnackedSplits.put(splitId, state.mySqlSplit().asSnapshotSplit());
            splitIdBelongSet.put(splitId, state.setInfo());
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
    }

    @Override
    public List<TdSqlSplit> snapshotState(long checkpointId) {
        List<TdSqlSplit> stateSplits = super.snapshotState(checkpointId);
        // unfinished splits
        List<TdSqlSplit> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .collect(Collectors.toList());

        addUnackedSplit(unfinishedSplits);

        return unfinishedSplits;
    }

    private void addUnackedSplit(List<TdSqlSplit> unfinishedSplits) {
        for (String splitId : finishedUnackedSplits.keySet()) {
            TdSqlSet set = splitIdBelongSet.get(splitId);
            unfinishedSplits.add(new TdSqlSplit(set, finishedUnackedSplits.get(splitId)));
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<TdSqlSet, Map<String, BinlogOffset>> reportGroup = new HashMap<>();

            for (String tdSqlSplitId : finishedUnackedSplits.keySet()) {
                MySqlSnapshotSplit mySqlSnapshotSplit = finishedUnackedSplits.get(tdSqlSplitId);
                TdSqlSet set = splitIdBelongSet.get(tdSqlSplitId);

                Map<String, BinlogOffset> finishedOffsets =
                        reportGroup.getOrDefault(set, new HashMap<>());
                finishedOffsets.put(
                        mySqlSnapshotSplit.splitId(), mySqlSnapshotSplit.getHighWatermark());
                LOGGER.trace("finished offset: {}", JSON.toString(finishedOffsets));
                reportGroup.put(set, finishedOffsets);
            }

            for (TdSqlSet set : reportGroup.keySet()) {
                final Map<String, BinlogOffset> finishedOffsets = reportGroup.get(set);
                FinishedSnapshotSplitsReportEvent mySqlReportEvent =
                        new FinishedSnapshotSplitsReportEvent(finishedOffsets);
                TdSqlSourceEvent tdSqlSourceEvent = new TdSqlSourceEvent(mySqlReportEvent, set);
                context.sendSourceEventToCoordinator(tdSqlSourceEvent);
            }
        } else {
            LOGGER.info("finished but unacknowledged collection is empty.");
        }
    }

    private void requestBinlogSplitMetaIfNeeded(TdSqlSplit tdSqlSplit) {
        if (tdSqlSplit.isSnapshotSplit()) {
            return;
        }
        final String tdSqlSplitId = tdSqlSplit.splitId();
        MySqlBinlogSplit binlogSplit = tdSqlSplit.mySqlSplit().asBinlogSplit();
        if (!binlogSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    ChunkUtils.getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            this.sourceConfigFunction
                                    .apply(tdSqlSplit.setInfo())
                                    .getSplitMetaGroupSize());
            BinlogSplitMetaRequestEvent splitMetaRequestEvent =
                    new BinlogSplitMetaRequestEvent(tdSqlSplitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(
                    new TdSqlSourceEvent(splitMetaRequestEvent, tdSqlSplit.setInfo()));
        } else {
            LOGGER.info("The meta of binlog split {} has been collected success", tdSqlSplitId);
            this.addSplits(Collections.singletonList(tdSqlSplit));
        }
    }

    private TdSqlSplit discoverTableSchemasForBinlogSplit(TdSqlSplit tdSqlSplit) {
        if (tdSqlSplit.isSnapshotSplit()) {
            return tdSqlSplit;
        }
        final String splitId = tdSqlSplit.splitId();
        MySqlBinlogSplit binlogSplit = tdSqlSplit.mySqlSplit().asBinlogSplit();
        if (binlogSplit.getTableSchemas().isEmpty()) {
            MySqlSourceConfig sourceConfig = sourceConfigFunction.apply(tdSqlSplit.setInfo());
            try (MySqlConnection jdbc =
                    DebeziumUtils.createMySqlConnection(sourceConfig.getDbzConfiguration())) {
                Map<TableId, TableChanges.TableChange> tableSchemas =
                        TableDiscoveryUtils.discoverCapturedTableSchemas(sourceConfig, jdbc);
                LOGGER.info("The table schema discovery for binlog split {} success", splitId);
                binlogSplit = MySqlBinlogSplit.fillTableSchemas(binlogSplit, tableSchemas);
                return new TdSqlSplit(tdSqlSplit.setInfo(), binlogSplit);
            } catch (SQLException e) {
                LOGGER.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOGGER.warn(
                    "The binlog split {} has table schemas yet, skip the table schema discovery",
                    tdSqlSplit);
            return tdSqlSplit;
        }
    }

    private void fillMetaDataForBinlogSplit(TdSqlSet set, BinlogSplitMetaEvent metadataEvent) {
        final String tdSqlSplitId = metadataEvent.getSplitId();

        MySqlBinlogSplit binlogSplit = uncompletedBinlogSplits.get(tdSqlSplitId);
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfigFunction.apply(set).getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo> metaDataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(FinishedSnapshotSplitInfo::deserialize)
                                .collect(Collectors.toList());

                binlogSplit = MySqlBinlogSplit.appendFinishedSplitInfos(binlogSplit, metaDataGroup);

                uncompletedBinlogSplits.put(tdSqlSplitId, binlogSplit);

                LOGGER.info("Fill meta data of group {} to binlog split", metaDataGroup.size());
            } else {
                LOGGER.warn(
                        "Received out of oder binlog meta event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(new TdSqlSplit(set, binlogSplit));
        }
    }
}
