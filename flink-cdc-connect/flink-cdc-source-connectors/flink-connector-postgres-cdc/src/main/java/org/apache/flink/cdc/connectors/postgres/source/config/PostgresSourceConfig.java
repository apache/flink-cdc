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
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;

/** The configuration for Postgres CDC source. */
public class PostgresSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final int lsnCommitCheckpointsDelay;

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
            @Nullable Map<ObjectPath, String> chunkKeyColumns,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            int lsnCommitCheckpointsDelay,
            boolean assignUnboundedChunkFirst) {

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
                chunkKeyColumns,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);

        this.subtaskId = subtaskId;
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;
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

    public Map<ObjectPath, String> getChunkKeyColumns() {
        return chunkKeyColumns;
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
        return new PostgresConnectorConfig(getDbzConfiguration());
    }
}
