/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_SERVER_NAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.PORT;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_TIME_ZONE;

/** The configuration properties that used by {@link MySqlParallelSource}. */
@Internal
public class MySqlParallelSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String DATABASE_SERVER_ID = "database.server.id";

    private final String startupMode;
    private final boolean includeSchemaChanges;
    private final String serverIdRange;
    private final int splitSize;
    /** The properties that {@link DebeziumReader} would use. */
    private final Properties dbzProperties;

    private MySqlParallelSourceConfig(
            String startupMode,
            boolean includeSchemaChanges,
            String serverIdRange,
            int splitSize,
            Properties dbzProperties) {
        this.startupMode = startupMode;
        this.includeSchemaChanges = includeSchemaChanges;
        this.serverIdRange = serverIdRange;
        this.splitSize = splitSize;
        this.dbzProperties = dbzProperties;
    }

    public String getStartupMode() {
        return startupMode;
    }

    public boolean includeSchemaChanges() {
        return includeSchemaChanges;
    }

    public String getServerIdRange() {
        return serverIdRange;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public Properties getDbzProperties() {
        return dbzProperties;
    }

    public Configuration getDbzConfig() {
        return Configuration.from(dbzProperties);
    }

    public MySqlConnectorConfig getMySqlConnectorConfig() {
        return new MySqlConnectorConfig(Configuration.from(dbzProperties));
    }

    public RelationalTableFilters getTableFilters() {
        return new MySqlConnectorConfig(Configuration.from(dbzProperties)).getTableFilters();
    }

    /** Builder for {@link MySqlParallelSourceConfig}. */
    public static final class Builder {

        private int splitSize = SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue();
        private String startupMode = SCAN_STARTUP_MODE.defaultValue();
        private boolean includeSchemaChanges = false;
        private String serverIdRange;
        /** The properties that {@link DebeziumReader} would use. */
        private Properties dbzProperties;

        public Builder() {
            // the default properties that debezium reader required.
            final Properties props = new Properties();
            props.put("database.server.name", DATABASE_SERVER_NAME);
            props.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
            props.put("database.history.instance.name", UUID.randomUUID().toString());
            props.put("database.history.skip.unparseable.ddl", String.valueOf(true));
            props.put("database.history.refer.ddl", String.valueOf(true));
            props.put("tombstones.on.delete", String.valueOf(false));
            props.put("database.responseBuffering", "adaptive");
            props.put("snapshot.mode", SCAN_STARTUP_MODE.defaultValue());
            props.put("database.port", String.valueOf(PORT.defaultValue()));
            props.put(
                    "database.fetchSize", String.valueOf(SCAN_SNAPSHOT_FETCH_SIZE.defaultValue()));
            props.put("database.serverTimezone", SERVER_TIME_ZONE.defaultValue());
            props.put(
                    "connect.timeout.ms",
                    String.valueOf(CONNECT_TIMEOUT.defaultValue().toMillis()));

            this.dbzProperties = props;
        }

        public Builder startupMode(String startupMode) {
            this.startupMode = startupMode;
            return this;
        }

        public Builder includeSchemaChanges(boolean includeSchemaChanges) {
            this.includeSchemaChanges = includeSchemaChanges;
            return this;
        }

        public Builder serverIdRange(String serverIdRange) {
            this.serverIdRange = serverIdRange;
            return this;
        }

        public Builder splitSize(int splitSize) {
            this.splitSize = splitSize;
            return this;
        }

        public Builder capturedDatabases(String capturedDatabases) {
            if (capturedDatabases != null) {
                dbzProperties.put("database.whitelist", capturedDatabases);
            }
            return this;
        }

        public Builder capturedTables(String capturedTables) {
            if (capturedTables != null) {
                dbzProperties.put("table.whitelist", capturedTables);
            }
            return this;
        }

        public Builder hostname(String hostname) {
            dbzProperties.put("database.hostname", hostname);
            return this;
        }

        public Builder username(String username) {
            dbzProperties.put("database.user", username);
            return this;
        }

        public Builder password(String password) {
            dbzProperties.put("database.password", password);
            return this;
        }

        public Builder port(int port) {
            dbzProperties.put("database.port", String.valueOf(port));
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            dbzProperties.put("connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            dbzProperties.put("database.fetchSize", String.valueOf(fetchSize));
            return this;
        }

        public Builder serverTimeZone(String serverTimeZone) {
            if (serverTimeZone != null) {
                dbzProperties.put("database.serverTimezone", serverTimeZone);
            }
            return this;
        }

        public Builder dbzProperties(Properties dbzProperties) {
            this.dbzProperties.putAll(dbzProperties);
            return this;
        }

        public MySqlParallelSourceConfig build() {
            return new MySqlParallelSourceConfig(
                    startupMode, includeSchemaChanges, serverIdRange, splitSize, dbzProperties);
        }
    }

    // --------------------------------------------
    // utils for debezium reader
    // --------------------------------------------
    public static BinaryLogClient getBinaryClient(Configuration dbzConfiguration) {
        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(dbzConfiguration);
        return new BinaryLogClient(
                connectorConfig.hostname(),
                connectorConfig.port(),
                connectorConfig.username(),
                connectorConfig.password());
    }

    public static MySqlConnection getConnection(Configuration dbzConfiguration) {
        return new MySqlConnection(
                new MySqlConnection.MySqlConnectionConfiguration(dbzConfiguration));
    }

    public static MySqlDatabaseSchema getMySqlDatabaseSchema(
            MySqlConnectorConfig connectorConfig, MySqlConnection connection) {
        boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        MySqlValueConverters valueConverters = getValueConverters(connectorConfig);
        return new MySqlDatabaseSchema(
                connectorConfig,
                valueConverters,
                topicSelector,
                schemaNameAdjuster,
                tableIdCaseInsensitive);
    }

    public static MySqlValueConverters getValueConverters(MySqlConnectorConfig connectorConfig) {
        TemporalPrecisionMode timePrecisionMode = connectorConfig.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = connectorConfig.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
                connectorConfig
                        .getConfig()
                        .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                        bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
                bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        boolean timeAdjusterEnabled =
                connectorConfig.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(
                decimalMode,
                timePrecisionMode,
                bigIntUnsignedMode,
                connectorConfig.binaryHandlingMode(),
                timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
    }

    public static Optional<String> getServerIdForSubTask(String serverIdRange, int subtaskId) {
        if (serverIdRange == null) {
            return Optional.empty();
        }
        if (serverIdRange.contains("-")) {
            int serverIdStart = Integer.parseInt(serverIdRange.split("-")[0].trim());
            int serverIdEnd = Integer.parseInt(serverIdRange.split("-")[1].trim());
            int serverId = serverIdStart + subtaskId;
            Preconditions.checkState(
                    serverIdStart <= serverId && serverId <= serverIdEnd,
                    String.format(
                            "The server id %s in task %d is out of server id range %s, please keep the job parallelism same with server id num of server id range.",
                            serverId, subtaskId, serverIdRange));
            return Optional.of(String.valueOf(serverId));
        } else {
            int serverIdStart = Integer.parseInt(serverIdRange);
            if (subtaskId > 0) {
                throw new IllegalStateException(
                        String.format(
                                "The server id should a range like '5400-5404' when %s enabled , but actual is %s",
                                SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key(), serverIdRange));
            } else {
                return Optional.of(String.valueOf(serverIdStart));
            }
        }
    }

    @Override
    public String toString() {
        return "MySqlParallelSourceConfig{"
                + "startupMode='"
                + startupMode
                + '\''
                + ", splitSize="
                + splitSize
                + ", serverIdRange='"
                + serverIdRange
                + '\''
                + ", dbzProperties="
                + dbzProperties
                + '}';
    }
}
