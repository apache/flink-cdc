package com.ververica.cdc.connectors.tdsql.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;
import com.ververica.cdc.connectors.tdsql.source.events.TdSqlSourceEvent;
import com.ververica.cdc.connectors.tdsql.source.split.TdSqlSplitState;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The source reader for TdSql source splits.
 *
 * @param <T> The output type for flink.
 */
public class TdSqlSourceReader<T>
        extends SourceReaderBase<SourceRecord, T, TdSqlSplit, TdSqlSplitState> {
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, TdSqlSet> splitIdBelongSet;

    public TdSqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue,
            SplitFetcherManager<SourceRecord, TdSqlSplit> splitFetcherManager,
            RecordEmitter<SourceRecord, T, TdSqlSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        this.finishedUnackedSplits = new HashMap<>();
        this.splitIdBelongSet = new HashMap<>();
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
                unfinishedSplits.add(split);
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

        super.addSplits(unfinishedSplits);
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

            if (mySqlSourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
                FinishedSnapshotSplitsAckEvent ackEvent =
                        (FinishedSnapshotSplitsAckEvent) mySqlSourceEvent;
                for (String splitId : ackEvent.getFinishedSplits()) {
                    this.finishedUnackedSplits.remove(splitId);
                    this.splitIdBelongSet.remove(splitId);
                }
            } else if (mySqlSourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
                reportFinishedSnapshotSplitsIfNeed();
            }
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    protected void onSplitFinished(Map<String, TdSqlSplitState> finishedSplitIds) {
        for (String splitId : finishedSplitIds.keySet()) {
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

            for (String splitId : finishedUnackedSplits.keySet()) {
                MySqlSnapshotSplit mySqlSnapshotSplit = finishedUnackedSplits.get(splitId);
                TdSqlSet set = splitIdBelongSet.get(splitId);

                Map<String, BinlogOffset> finishedOffsets =
                        reportGroup.getOrDefault(set, new HashMap<>());

                finishedOffsets.put(
                        mySqlSnapshotSplit.splitId(), mySqlSnapshotSplit.getHighWatermark());
                reportGroup.put(set, finishedOffsets);
            }

            for (TdSqlSet set : reportGroup.keySet()) {
                final Map<String, BinlogOffset> finishedOffsets = reportGroup.get(set);
                FinishedSnapshotSplitsReportEvent mySqlReportEvent =
                        new FinishedSnapshotSplitsReportEvent(finishedOffsets);
                TdSqlSourceEvent tdSqlSourceEvent = new TdSqlSourceEvent(mySqlReportEvent, set);
                context.sendSourceEventToCoordinator(tdSqlSourceEvent);
            }
        }
    }
}
