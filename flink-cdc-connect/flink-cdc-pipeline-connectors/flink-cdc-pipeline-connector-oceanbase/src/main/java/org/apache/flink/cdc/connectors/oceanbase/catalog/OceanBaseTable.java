/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oceanbase.catalog;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Describe a OceanBase table. */
public class OceanBaseTable {

    /** Types of OceanBase table. */
    public enum TableType {
        UNKNOWN,
        DUPLICATE_KEY,
        PRIMARY_KEY
    }

    /** The database name. */
    private final String databaseName;

    /** The table name. */
    private final String tableName;

    /** The type of OceanBase type. */
    private final TableType tableType;

    /** The columns sorted by the ordinal position. */
    private final List<OceanBaseColumn> columns;

    @Nullable private final List<String> tableKeys;

    private final List<String> partitionKeys;

    /** The table comment. null if there is no comment or it's unknown. */
    @Nullable private final String comment;

    /** The properties of the table. */
    private final Map<String, String> properties;

    /** Map the column name to the column. May be lazily initialized. */
    @Nullable private volatile Map<String, OceanBaseColumn> columnMap;

    private OceanBaseTable(
            String databaseName,
            String tableName,
            TableType tableType,
            List<OceanBaseColumn> columns,
            @Nullable List<String> tableKeys,
            @Nullable List<String> partitionKeys,
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
        this.partitionKeys = partitionKeys;
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

    public List<OceanBaseColumn> getColumns() {
        return columns;
    }

    public Optional<List<String>> getTableKeys() {
        return Optional.ofNullable(tableKeys);
    }

    public Optional<List<String>> getPartitionKeys() {
        return Optional.ofNullable(partitionKeys);
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public OceanBaseColumn getColumn(String columnName) {
        if (columnMap == null) {
            synchronized (this) {
                if (columnMap == null) {
                    columnMap = new HashMap<>();
                    for (OceanBaseColumn column : columns) {
                        columnMap.put(column.getColumnName(), column);
                    }
                }
            }
        }
        return columnMap.get(columnName);
    }

    @Override
    public String toString() {
        return "OceanBaseTable{"
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
                + ", partitionKeys="
                + partitionKeys
                + ", comment='"
                + comment
                + '\''
                + ", properties="
                + properties
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OceanBaseTable that = (OceanBaseTable) o;
        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName)
                && tableType == that.tableType
                && Objects.equals(columns, that.columns)
                && Objects.equals(tableKeys, that.tableKeys)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(comment, that.comment)
                && Objects.equals(properties, that.properties);
    }

    /** Build a {@link OceanBaseTable}. */
    public static class Builder {

        private String databaseName;
        private String tableName;
        private TableType tableType;
        private List<OceanBaseColumn> columns = new ArrayList<>();
        private List<String> tableKeys;
        private List<String> partitionKeys;
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

        public Builder setColumns(List<OceanBaseColumn> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setTableKeys(List<String> tableKeys) {
            this.tableKeys = tableKeys;
            return this;
        }

        public Builder setPartitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
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

        public OceanBaseTable build() {
            return new OceanBaseTable(
                    databaseName,
                    tableName,
                    tableType,
                    columns,
                    tableKeys,
                    partitionKeys,
                    comment,
                    properties);
        }
    }
}
