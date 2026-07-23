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

package org.apache.flink.cdc.connectors.postgres.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Tables;
import io.debezium.util.Strings;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;

/** The configuration for Postgres CDC source. */
public class PostgresSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final int lsnCommitCheckpointsDelay;
    private final boolean includePartitionedTables;
    private final boolean includeDatabaseInTableId;
    private final List<String> logicalMessagePrefixes;

    public PostgresSourceConfig(
            int subtaskId,
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> schemaList,
            List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Configuration dbzConfiguration,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            @Nullable String chunkKeyColumn,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            int lsnCommitCheckpointsDelay,
            boolean assignUnboundedChunkFirst,
            boolean includePartitionedTables,
            boolean includeDatabaseInTableId,
            List<String> logicalMessagePrefixes) {
        super(
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
        this.subtaskId = subtaskId;
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;
        this.includePartitionedTables = includePartitionedTables;
        this.includeDatabaseInTableId = includeDatabaseInTableId;
        this.logicalMessagePrefixes = logicalMessagePrefixes;
    }

    /**
     * Returns {@code subtaskId} value.
     *
     * @return subtask id
     */
    public int getSubtaskId() {
        return subtaskId;
    }

    /**
     * Returns {@code lsnCommitCheckpointsDelay} value.
     *
     * @return lsn commit checkpoint delay
     */
    public int getLsnCommitCheckpointsDelay() {
        return this.lsnCommitCheckpointsDelay;
    }

    /**
     * Returns {@code includePartitionedTables} value.
     *
     * @return include partitioned table
     */
    public boolean includePartitionedTables() {
        return includePartitionedTables;
    }

    /**
     * Returns the slot name for backfill task.
     *
     * @return backfill task slot name
     */
    public String getSlotNameForBackfillTask() {
        return getDbzProperties().getProperty(SLOT_NAME.name()) + "_" + getSubtaskId();
    }

    /** Returns the JDBC URL for config unique key. */
    public String getJdbcUrl() {
        return String.format(
                "jdbc:postgresql://%s:%d/%s", getHostname(), getPort(), getDatabaseList().get(0));
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return applyCaseSensitiveTableFilter(new PostgresConnectorConfig(getDbzConfiguration()));
    }

    /** Applies the case-sensitive Postgres table filter to the connector configuration. */
    public PostgresConnectorConfig applyCaseSensitiveTableFilter(
            PostgresConnectorConfig connectorConfig) {
        String tableIncludeList = connectorConfig.getConfig().getString("table.include.list");
        if (tableIncludeList != null && !tableIncludeList.trim().isEmpty()) {
            RelationalTableFilters tableFilters = connectorConfig.getTableFilters();
            Tables.TableFilter originalTableFilter = tableFilters.dataCollectionFilter();
            Set<Pattern> tableIncludePatterns = Strings.setOfRegex(tableIncludeList);
            tableFilters.setDataCollectionFilters(
                    tableId ->
                            originalTableFilter.isIncluded(tableId)
                                    && tableIncludePatterns.stream()
                                            .anyMatch(
                                                    pattern ->
                                                            pattern.matcher(
                                                                            tableId.schema()
                                                                                    + "."
                                                                                    + tableId
                                                                                            .table())
                                                                    .matches()));
        }
        return connectorConfig;
    }

    /** Returns whether to include database in the generated Table ID. */
    public boolean isIncludeDatabaseInTableId() {
        return includeDatabaseInTableId;
    }

    /** Returns the prefixes for Postgres logical decoding messages. */
    public List<String> getLogicalMessagePrefixes() {
        return logicalMessagePrefixes;
    }
}
