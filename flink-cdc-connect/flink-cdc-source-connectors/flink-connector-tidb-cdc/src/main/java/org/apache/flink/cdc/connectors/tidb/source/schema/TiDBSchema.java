package org.apache.flink.cdc.connectors.tidb.source.schema;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.Map;

public class TiDBSchema {
    private final Map<TableId, TableChanges.TableChange> schemasByTableId;

    public TiDBSchema(Map<TableId, TableChanges.TableChange> schemasByTableId) {
        this.schemasByTableId = schemasByTableId;
    }
}
