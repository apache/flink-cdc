package org.apache.flink.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

public class BinlogNewAddedTableEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private String catalog;
    private String schema;
    private String table;

    public BinlogNewAddedTableEvent(String catalog, String schema, String table) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

}
