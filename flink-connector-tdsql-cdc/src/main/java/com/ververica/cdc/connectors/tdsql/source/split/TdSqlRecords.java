package com.ververica.cdc.connectors.tdsql.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of {@link RecordsWithSplitIds} which contains the records of one table split.
 */
public class TdSqlRecords implements RecordsWithSplitIds<SourceRecord> {
    private final RecordsWithSplitIds<SourceRecord> mySqlRecords;
    private final TdSqlSet set;

    public TdSqlRecords(RecordsWithSplitIds<SourceRecord> mySqlRecords, TdSqlSet set) {
        this.mySqlRecords = mySqlRecords;
        this.set = set;
    }

    @Nullable
    @Override
    public String nextSplit() {
        String mysqlSplitId = mySqlRecords.nextSplit();
        if (mysqlSplitId == null) {
            return null;
        }
        return tdSqlSplitId(mysqlSplitId);
    }

    @Nullable
    @Override
    public SourceRecord nextRecordFromSplit() {
        return mySqlRecords.nextRecordFromSplit();
    }

    @Override
    public Set<String> finishedSplits() {
        Set<String> finishedSplits = mySqlRecords.finishedSplits();
        return finishedSplits.stream().map(this::tdSqlSplitId).collect(Collectors.toSet());
    }

    private String tdSqlSplitId(String mysqlSplitId) {
        return set.getSetKey() + ":" + mysqlSplitId;
    }
}
