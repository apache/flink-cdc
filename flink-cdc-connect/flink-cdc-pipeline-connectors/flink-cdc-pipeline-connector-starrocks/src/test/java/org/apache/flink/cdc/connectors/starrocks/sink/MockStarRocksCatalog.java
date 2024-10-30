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

package org.apache.flink.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import com.starrocks.connector.flink.catalog.StarRocksTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Mock {@link StarRocksCatalog} for testing. */
public class MockStarRocksCatalog extends StarRocksEnrichedCatalog {

    /** database name -> table name -> table. */
    private final Map<String, Map<String, StarRocksTable>> tables;

    public MockStarRocksCatalog() {
        super("jdbc:mysql://127.0.0.1:9030", "root", "");
        this.tables = new HashMap<>();
    }

    @Override
    public void open() throws StarRocksCatalogException {
        // do nothing
    }

    @Override
    public void close() throws StarRocksCatalogException {
        // do nothing
    }

    @Override
    public boolean databaseExists(String databaseName) throws StarRocksCatalogException {
        return tables.containsKey(databaseName);
    }

    @Override
    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws StarRocksCatalogException {
        if (!tables.containsKey(databaseName)) {
            tables.put(databaseName, new HashMap<>());
        } else if (!ignoreIfExists) {
            throw new StarRocksCatalogException(
                    String.format("database %s already exists", databaseName));
        }
    }

    @Override
    public Optional<StarRocksTable> getTable(String databaseName, String tableName)
            throws StarRocksCatalogException {
        if (!tables.containsKey(databaseName)) {
            return Optional.empty();
        }

        StarRocksTable table = tables.get(databaseName).get(tableName);
        return Optional.ofNullable(table);
    }

    @Override
    public void createTable(StarRocksTable table, boolean ignoreIfExists)
            throws StarRocksCatalogException {
        String databaseName = table.getDatabaseName();
        String tableName = table.getTableName();
        Map<String, StarRocksTable> dbTables = tables.get(databaseName);
        if (dbTables == null) {
            throw new StarRocksCatalogException(
                    String.format("database %s does not exist", databaseName));
        }

        StarRocksTable oldTable = dbTables.get(tableName);
        if (oldTable == null) {
            dbTables.put(tableName, table);
        } else if (!ignoreIfExists) {
            throw new StarRocksCatalogException(
                    String.format("table %s.%s already exists", databaseName, tableName));
        }
    }

    @Override
    public void alterAddColumns(
            String databaseName,
            String tableName,
            List<StarRocksColumn> addColumns,
            long timeoutSecond)
            throws StarRocksCatalogException {
        Map<String, StarRocksTable> dbTables = tables.get(databaseName);
        if (dbTables == null) {
            throw new StarRocksCatalogException(
                    String.format("database %s does not exist", databaseName));
        }

        StarRocksTable oldTable = dbTables.get(tableName);
        if (oldTable == null) {
            throw new StarRocksCatalogException(
                    String.format("table %s.%s does not exist", databaseName, tableName));
        }

        List<StarRocksColumn> newColumns = new ArrayList<>(oldTable.getColumns());
        for (StarRocksColumn column : addColumns) {
            StarRocksColumn newColumn =
                    new StarRocksColumn.Builder()
                            .setColumnName(column.getColumnName())
                            .setOrdinalPosition(newColumns.size())
                            .setDataType(column.getDataType())
                            .setNullable(column.isNullable())
                            .setDefaultValue(column.getDefaultValue().orElse(null))
                            .setColumnSize(column.getColumnSize().orElse(null))
                            .setDecimalDigits(column.getDecimalDigits().orElse(null))
                            .setColumnComment(column.getColumnComment().orElse(null))
                            .build();
            newColumns.add(newColumn);
        }

        StarRocksTable newTable =
                new StarRocksTable.Builder()
                        .setDatabaseName(oldTable.getDatabaseName())
                        .setTableName(oldTable.getTableName())
                        .setTableType(oldTable.getTableType())
                        .setColumns(newColumns)
                        .setTableKeys(oldTable.getTableKeys().orElse(null))
                        .setDistributionKeys(oldTable.getDistributionKeys().orElse(null))
                        .setNumBuckets(oldTable.getNumBuckets().orElse(null))
                        .setComment(oldTable.getComment().orElse(null))
                        .setTableProperties(oldTable.getProperties())
                        .build();
        dbTables.put(tableName, newTable);
    }

    @Override
    public void alterDropColumns(
            String databaseName, String tableName, List<String> dropColumns, long timeoutSecond)
            throws StarRocksCatalogException {
        Map<String, StarRocksTable> dbTables = tables.get(databaseName);
        if (dbTables == null) {
            throw new StarRocksCatalogException(
                    String.format("database %s does not exist", databaseName));
        }

        StarRocksTable oldTable = dbTables.get(tableName);
        if (oldTable == null) {
            throw new StarRocksCatalogException(
                    String.format("table %s.%s does not exist", databaseName, tableName));
        }

        List<StarRocksColumn> newColumns = new ArrayList<>();
        for (StarRocksColumn column : oldTable.getColumns()) {
            if (dropColumns.contains(column.getColumnName())) {
                continue;
            }
            StarRocksColumn newColumn =
                    new StarRocksColumn.Builder()
                            .setColumnName(column.getColumnName())
                            .setOrdinalPosition(newColumns.size())
                            .setDataType(column.getDataType())
                            .setNullable(column.isNullable())
                            .setDefaultValue(column.getDefaultValue().orElse(null))
                            .setColumnSize(column.getColumnSize().orElse(null))
                            .setDecimalDigits(column.getDecimalDigits().orElse(null))
                            .setColumnComment(column.getColumnComment().orElse(null))
                            .build();
            newColumns.add(newColumn);
        }

        StarRocksTable newTable =
                new StarRocksTable.Builder()
                        .setDatabaseName(oldTable.getDatabaseName())
                        .setTableName(oldTable.getTableName())
                        .setTableType(oldTable.getTableType())
                        .setColumns(newColumns)
                        .setTableKeys(oldTable.getTableKeys().orElse(null))
                        .setDistributionKeys(oldTable.getDistributionKeys().orElse(null))
                        .setNumBuckets(oldTable.getNumBuckets().orElse(null))
                        .setComment(oldTable.getComment().orElse(null))
                        .setTableProperties(oldTable.getProperties())
                        .build();
        dbTables.put(tableName, newTable);
    }
}
