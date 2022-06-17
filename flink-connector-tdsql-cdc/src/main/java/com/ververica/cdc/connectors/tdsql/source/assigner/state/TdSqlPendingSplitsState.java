package com.ververica.cdc.connectors.tdsql.source.assigner.state;

import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/** A {@link PendingSplitsState} for pending mysql pending state(snapshot & binlog) splits. */
public class TdSqlPendingSplitsState extends PendingSplitsState {
    private final Map<TdSqlSet, PendingSplitsState> stateMap;

    @Nullable private transient byte[] serializedCache;

    public TdSqlPendingSplitsState(Map<TdSqlSet, PendingSplitsState> stateMap) {
        this.stateMap = stateMap;
    }

    public Map<TdSqlSet, PendingSplitsState> getStateMap() {
        return stateMap;
    }

    @Nullable
    public byte[] getSerializedCache() {
        return serializedCache;
    }

    public void setSerializedCache(@Nullable byte[] serializedCache) {
        this.serializedCache = serializedCache;
    }

    public boolean isSerialized() {
        return this.serializedCache != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TdSqlPendingSplitsState)) {
            return false;
        }
        TdSqlPendingSplitsState that = (TdSqlPendingSplitsState) o;
        return Objects.equals(getStateMap(), that.getStateMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStateMap());
    }

    @Override
    public String toString() {
        return "TdSqlPendingSplitsState{" + "stateMap=" + stateMap + '}';
    }
}
