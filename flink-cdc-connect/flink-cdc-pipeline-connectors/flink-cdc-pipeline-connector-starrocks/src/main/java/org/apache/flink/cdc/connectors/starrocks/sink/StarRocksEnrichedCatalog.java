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
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "Table name cannot be null or empty.");
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
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "Table name cannot be null or empty.");
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

    private String buildTruncateTableSql(String databaseName, String tableName) {
        return String.format("TRUNCATE TABLE `%s`.`%s`;", databaseName, tableName);
    }

    private String buildDropTableSql(String databaseName, String tableName) {
        return String.format("DROP TABLE `%s`.`%s`;", databaseName, tableName);
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
}
