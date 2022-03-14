package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;

/** Defines the supported metadata columns for {@link TiDBTableSource}. */
public class TiKVReadableMetadata {

    private final String key;

    private final DataType dataType;

    private final TiKVMetadataConverter converter;

    TiKVReadableMetadata(String key, DataType dataType, TiKVMetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public TiKVMetadataConverter getConverter() {
        return converter;
    }

    /** Name of the table that contain the row. */
    public static TiKVReadableMetadata createTableNameMetadata(String tableName) {
        return new TiKVReadableMetadata(
                "table_name",
                DataTypes.STRING().notNull(),
                new TiKVMetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(TiKVRowValue row) {
                        return StringData.fromString(tableName);
                    }
                });
    }

    /** Name of the database that contain the row. */
    public static TiKVReadableMetadata createDatabaseNameMetadata(String database) {
        return new TiKVReadableMetadata(
                "database_name",
                DataTypes.STRING().notNull(),
                new TiKVMetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(TiKVRowValue row) {
                        return StringData.fromString(database);
                    }
                });
    }

    public static TiKVReadableMetadata createOpTsMetadata() {
        return new TiKVReadableMetadata(
                "op_ts",
                DataTypes.TIMESTAMP_LTZ(3).notNull(),
                new TiKVMetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(TiKVRowValue row) {
                        if (row.isKv) {
                            // We cannot get ts from KvPair, use default value.
                            return TimestampData.fromEpochMillis(0);
                        } else {
                            return TimestampData.fromEpochMillis(row.row.getStartTs());
                        }
                    }
                });
    }

    public static TiKVReadableMetadata[] createTiKVReadableMetadata(
            String database, String tableName) {
        List<TiKVReadableMetadata> list = new ArrayList<>();
        list.add(createDatabaseNameMetadata(database));
        list.add(createTableNameMetadata(tableName));
        list.add(createOpTsMetadata());
        return list.toArray(new TiKVReadableMetadata[0]);
    }
}
