package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.table.data.GenericRowData;

import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergEvent {

    // Identifier for the iceberg table to be written.
    TableIdentifier tableId;

    // The actual record to be written to iceberg table.
    GenericRowData genericRow;

    // if true, means that table schema has changed right before this genericRow.
    boolean shouldRefreshSchema;

    public IcebergEvent(
            TableIdentifier tableId, GenericRowData genericRow, boolean shouldRefreshSchema) {
        this.tableId = tableId;
        this.genericRow = genericRow;
        this.shouldRefreshSchema = shouldRefreshSchema;
    }

    public IcebergEvent(TableIdentifier tableId, GenericRowData genericRow) {
        this.tableId = tableId;
        this.genericRow = genericRow;
        this.shouldRefreshSchema = false;
    }

    public TableIdentifier getTableId() {
        return tableId;
    }

    public void setTableId(TableIdentifier tableId) {
        this.tableId = tableId;
    }

    public GenericRowData getGenericRow() {
        return genericRow;
    }

    public void setGenericRow(GenericRowData genericRow) {
        this.genericRow = genericRow;
    }

    public boolean isShouldRefreshSchema() {
        return shouldRefreshSchema;
    }

    public void setShouldRefreshSchema(boolean shouldRefreshSchema) {
        this.shouldRefreshSchema = shouldRefreshSchema;
    }
}
