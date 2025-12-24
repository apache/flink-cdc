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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** An enriched {@code StarRocksCatalog} with more schema evolution abilities. */
public class StarRocksEnrichedCatalog extends StarRocksCatalog {
    public StarRocksEnrichedCatalog(String jdbcUrl, String username, String password) {
        super(jdbcUrl, username, password);
    }

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksEnrichedCatalog.class);

    public void truncateTable(String databaseName, String tableName)
            throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        String alterSql = this.buildTruncateTableSql(databaseName, tableName);
        try {
            // TRUNCATE TABLE is not regarded as a column-based schema change for StarRocks, so
            // there's no need to check the evolution state.
            executeUpdateStatement(alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to truncate table `{}`.`{}`. SQL executed: {}",
                    databaseName,
                    tableName,
                    alterSql);
            throw new StarRocksCatalogException(
                    String.format("Failed to truncate table `%s`.`%s`.", databaseName, tableName),
                    e);
        }
    }

    public void dropTable(String databaseName, String tableName) throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        String alterSql = this.buildDropTableSql(databaseName, tableName);
        try {
            // like TRUNCATE TABLE, DROP TABLE isn't a column-affecting operation and `executeAlter`
            // method isn't appropriate.
            executeUpdateStatement(alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to drop table `{}`.`{}`. SQL executed: {}",
                    databaseName,
                    tableName,
                    alterSql);
            throw new StarRocksCatalogException(
                    String.format("Failed to drop table `%s`.`%s`.", databaseName, tableName), e);
        }
    }

    public void renameColumn(
            String databaseName, String tableName, String oldColumnName, String newColumnName)
            throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(oldColumnName),
                "old column name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(newColumnName),
                "new column name cannot be null or empty.");
        String alterSql =
                this.buildRenameColumnSql(databaseName, tableName, oldColumnName, newColumnName);
        try {
            executeUpdateStatement(alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to alter table `{}`.`{}` rename column {} to {}. SQL executed: {}",
                    databaseName,
                    tableName,
                    oldColumnName,
                    newColumnName,
                    alterSql);
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to alter table `%s`.`%s` rename column %s to %s.",
                            databaseName, tableName, oldColumnName, newColumnName),
                    e);
        }
    }

    public void alterColumnType(String databaseName, String tableName, StarRocksColumn column)
            throws StarRocksCatalogException {
        checkTableArgument(databaseName, tableName);
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(column.getColumnName()),
                "column name cannot be null or empty.");
        String alterSql =
                buildAlterColumnTypeSql(
                        databaseName, tableName, column.getColumnName(), column.getDataType());
        try {
            long startTimeMillis = System.currentTimeMillis();
            executeUpdateStatement(alterSql);
            LOG.info(
                    "Success to alter table {}.{} modify column type, duration: {}ms, sql: {}",
                    databaseName,
                    tableName,
                    System.currentTimeMillis() - startTimeMillis,
                    alterSql);
        } catch (Exception e) {
            LOG.error(
                    "Failed to alter table {}.{} modify column type, sql: {}",
                    databaseName,
                    tableName,
                    alterSql,
                    e);
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to alter table %s.%s modify column type",
                            databaseName, tableName),
                    e);
        }
    }

    private String buildTruncateTableSql(String databaseName, String tableName) {
        return String.format("TRUNCATE TABLE `%s`.`%s`;", databaseName, tableName);
    }

    private String buildDropTableSql(String databaseName, String tableName) {
        return String.format("DROP TABLE `%s`.`%s`;", databaseName, tableName);
    }

    private String buildRenameColumnSql(
            String databaseName, String tableName, String oldColumnName, String newColumnName) {
        return String.format(
                "ALTER TABLE `%s`.`%s` RENAME COLUMN %s TO %s;",
                databaseName, tableName, oldColumnName, newColumnName);
    }

    private String buildAlterColumnTypeSql(
            String databaseName, String tableName, String columnName, String dataType) {
        return String.format(
                "ALTER TABLE `%s`.`%s` MODIFY COLUMN %s %s",
                databaseName, tableName, columnName, dataType);
    }

    private void executeUpdateStatement(String sql) throws StarRocksCatalogException {
        try {
            Method m =
                    getClass()
                            .getSuperclass()
                            .getDeclaredMethod("executeUpdateStatement", String.class);
            m.setAccessible(true);
            m.invoke(this, sql);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkTableArgument(String databaseName, String tableName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "Table name cannot be null or empty.");
    }
}
