package org.apache.flink.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

public class BinlogNewAddedTableEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private String schema;
    private String db;
    private String table;

    public BinlogNewAddedTableEvent(String schema, String db, String table) {
        this.schema = schema;
        this.db = db;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

}
