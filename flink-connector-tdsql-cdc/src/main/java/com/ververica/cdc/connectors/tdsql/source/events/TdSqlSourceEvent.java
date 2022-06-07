package com.ververica.cdc.connectors.tdsql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;

public class TdSqlSourceEvent implements SourceEvent {
    private static final long serialVersionUID = -5194600926649657532L;

    private final SourceEvent mySqlEvent;
    private final TdSqlSet set;

    public TdSqlSourceEvent(SourceEvent mySqlEvent, TdSqlSet set) {
        this.mySqlEvent = mySqlEvent;
        this.set = set;
    }

    public SourceEvent getMySqlEvent() {
        return mySqlEvent;
    }

    public TdSqlSet getSet() {
        return set;
    }

    @Override
    public String toString() {
        return "TdSqlSourceEvent{" + "mySqlEvent=" + mySqlEvent + ", set=" + set + '}';
    }
}
