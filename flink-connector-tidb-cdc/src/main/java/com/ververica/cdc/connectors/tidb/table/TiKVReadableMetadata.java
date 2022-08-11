/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the change stream, the value is always 0.
     */
    public static TiKVReadableMetadata createOpTsMetadata() {
        return new TiKVReadableMetadata(
                "op_ts",
                DataTypes.TIMESTAMP_LTZ(3).notNull(),
                new TiKVMetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(TiKVRowValue row) {
                        if (row.isSnapshotRecord) {
                            // Uses OL as the operation time of snapshot records.
                            return TimestampData.fromEpochMillis(0L);
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
