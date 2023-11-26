/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.starrocks.sink;

import com.ververica.cdc.common.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Describe a StarRocks table. See <a
 * href="https://docs.starrocks.io/docs/table_design/StarRocks_table_design">StarRocks table
 * design</a> for how to define a StarRocks table.
 */
public class StarRocksTable {

    /**
     * Types of StarRocks table. See <a
     * href="https://docs.starrocks.io/docs/table_design/table_types">StarRocks Table Types</a>.
     */
    public enum TableType {
        UNKNOWN,
        DUPLICATE_KEY,
        AGGREGATE,
        UNIQUE_KEY,
        PRIMARY_KEY
    }

    /** The database name. */
    private final String databaseName;

    /** The table name. */
    private final String tableName;

    /** The type of StarRocks type. */
    private final TableType tableType;

    /** The columns sorted by the ordinal position. */
    private final List<StarRocksColumn> columns;

    /**
     * The table keys sorted by the ordinal position. null if it's unknown. The table keys has
     * different meaning for different types of tables. For duplicate key table, It's duplicate
     * keys. For aggregate table, it's aggregate keys. For unique key table, it's unique keys. For
     * primary key table, it's primary keys.
     */
    @Nullable private final List<String> tableKeys;

    /** The distribution keys. null if it's unknown. */
    @Nullable private final List<String> distributionKeys;

    /** The number of buckets. null if it's unknown or automatic. */
    @Nullable private final Integer numBuckets;

    /** The table comment. null if there is no comment or it's unknown. */
    @Nullable private final String comment;

    /** The properties of the table. */
    private final Map<String, String> properties;

    /** Map the column name to the column. May be lazily initialized. */
    @Nullable private volatile Map<String, StarRocksColumn> columnMap;

    private StarRocksTable(
            String databaseName,
            String tableName,
            TableType tableType,
            List<StarRocksColumn> columns,
            @Nullable List<String> tableKeys,
            @Nullable List<String> distributionKeys,
            @Nullable Integer numBuckets,
            @Nullable String comment,
            Map<String, String> properties) {
        Preconditions.checkNotNull(databaseName);
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(tableType);
        Preconditions.checkArgument(columns != null && !columns.isEmpty());
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.columns = columns;
        this.tableKeys = tableKeys;
        this.distributionKeys = distributionKeys;
        this.numBuckets = numBuckets;
        this.comment = comment;
        this.properties = Preconditions.checkNotNull(properties);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public TableType getTableType() {
        return tableType;
    }

    public List<StarRocksColumn> getColumns() {
        return columns;
    }

    public Optional<List<String>> getTableKeys() {
        return Optional.ofNullable(tableKeys);
    }

    public Optional<List<String>> getDistributionKeys() {
        return Optional.ofNullable(distributionKeys);
    }

    public Optional<Integer> getNumBuckets() {
        return Optional.ofNullable(numBuckets);
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public StarRocksColumn getColumn(String columnName) {
        if (columnMap == null) {
            synchronized (this) {
                if (columnMap == null) {
                    columnMap = new HashMap<>();
                    for (StarRocksColumn column : columns) {
                        columnMap.put(column.getColumnName(), column);
                    }
                }
            }
        }
        return columnMap.get(columnName);
    }

    @Override
    public String toString() {
        return "StarRocksTable{"
                + "databaseName='"
                + databaseName
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + ", tableType="
                + tableType
                + ", columns="
                + columns
                + ", tableKeys="
                + tableKeys
                + ", distributionKeys="
                + distributionKeys
                + ", numBuckets="
                + numBuckets
                + ", comment='"
                + comment
                + '\''
                + ", properties="
                + properties
                + '}';
    }

    /** Build a {@link StarRocksTable}. */
    public static class Builder {

        private String databaseName;
        private String tableName;
        private TableType tableType;
        private List<StarRocksColumn> columns = new ArrayList<>();
        private List<String> tableKeys;
        private List<String> distributionKeys;
        private Integer numBuckets;
        private String comment;
        private Map<String, String> properties = new HashMap<>();

        public Builder setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setTableType(TableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder setColumns(List<StarRocksColumn> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setTableKeys(List<String> tableKeys) {
            this.tableKeys = tableKeys;
            return this;
        }

        public Builder setDistributionKeys(List<String> distributionKeys) {
            this.distributionKeys = distributionKeys;
            return this;
        }

        public Builder setNumBuckets(int numBuckets) {
            this.numBuckets = numBuckets;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder setTableProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public StarRocksTable build() {
            return new StarRocksTable(
                    databaseName,
                    tableName,
                    tableType,
                    columns,
                    tableKeys,
                    distributionKeys,
                    numBuckets,
                    comment,
                    properties);
        }
    }
}
