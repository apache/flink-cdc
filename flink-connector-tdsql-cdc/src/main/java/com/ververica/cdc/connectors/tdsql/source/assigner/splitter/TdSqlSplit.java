package com.ververica.cdc.connectors.tdsql.source.assigner.splitter;

import org.apache.flink.api.connector.source.SourceSplit;

import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/** tdsql split. it contain mysql split info. */
public class TdSqlSplit implements SourceSplit {
    private final TdSqlSet setInfo;
    private final MySqlSplit mySqlSplit;
    @Nullable transient byte[] serializedFormCache;

    public TdSqlSplit(TdSqlSet setInfo, MySqlSplit mySqlSplit) {
        this.setInfo = setInfo;
        this.mySqlSplit = mySqlSplit;
    }

    public MySqlSplit mySqlSplit() {
        return mySqlSplit;
    }

    public TdSqlSet setInfo() {
        return setInfo;
    }

    public String setKey() {
        return setInfo.getSetKey();
    }

    public boolean isBinlogSplit() {
        return mySqlSplit.isBinlogSplit();
    }

    public boolean isSnapshotSplit() {
        return mySqlSplit().isSnapshotSplit();
    }

    @Override
    public String splitId() {
        return setInfo.getSetKey() + ":" + mySqlSplit.splitId();
    }

    @Override
    public String toString() {
        return "TdSqlSplit{" + "setInfo=" + setInfo + ", mySqlSplit=" + mySqlSplit + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TdSqlSplit)) {
            return false;
        }
        TdSqlSplit split = (TdSqlSplit) o;
        return Objects.equals(setInfo, split.setInfo)
                && Objects.equals(mySqlSplit, split.mySqlSplit)
                && Arrays.equals(serializedFormCache, split.serializedFormCache);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(setInfo, mySqlSplit);
        result = 31 * result + Arrays.hashCode(serializedFormCache);
        return result;
    }
}
