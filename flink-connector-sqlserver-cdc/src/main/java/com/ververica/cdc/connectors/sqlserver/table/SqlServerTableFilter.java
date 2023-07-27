package com.ververica.cdc.connectors.sqlserver.table;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * @Author zhangximing
 * @Date 2023/7/27 16:40
 * @Version 1.0
 */
public class SqlServerTableFilter implements Tables.TableFilter {

    private TableId tableId;

    public SqlServerTableFilter(TableId tableId) {
        this.tableId = tableId;
    }

    @Override
    public boolean isIncluded(TableId t) {
        return t.schema() != null && !(t.schema().equalsIgnoreCase("cdc") ||
                t.schema().equalsIgnoreCase("sys") ||
                t.table().equalsIgnoreCase("systranschemas"))
                && t.catalog().equalsIgnoreCase(tableId.catalog())
                && t.schema().equalsIgnoreCase(tableId.schema())
                && t.table().equalsIgnoreCase(tableId.table());
    }
}
