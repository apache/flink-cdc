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

package com.ververica.cdc.connectors.mysql.source.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ObjectPath;

import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.HEARTBEAT_INTERVAL;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.mysql.source.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A factory to construct {@link MySqlSourceConfig}. */
@Internal
public class MySqlSourceConfigFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private int port = 3306; // default 3306 port
    private String hostname;
    private String username;
    private String password;
    private ServerIdRange serverIdRange;
    private List<String> databaseList;
    private List<String> tableList;
    private String serverTimeZone = ZoneId.systemDefault().getId();
    private StartupOptions startupOptions = StartupOptions.initial();
    private int splitSize = SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue();
    private int splitMetaGroupSize = CHUNK_META_GROUP_SIZE.defaultValue();
    private int fetchSize = SCAN_SNAPSHOT_FETCH_SIZE.defaultValue();
    private Duration connectTimeout = CONNECT_TIMEOUT.defaultValue();
    private int connectMaxRetries = CONNECT_MAX_RETRIES.defaultValue();
    private int connectionPoolSize = CONNECTION_POOL_SIZE.defaultValue();
    private double distributionFactorUpper =
            CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue();
    private double distributionFactorLower =
            CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue();
    private boolean includeSchemaChanges = false;
    private boolean scanNewlyAddedTableEnabled = false;
    private boolean closeIdleReaders = false;
    private Properties jdbcProperties;
    private Duration heartbeatInterval = HEARTBEAT_INTERVAL.defaultValue();
    private Properties dbzProperties;
    private Map<ObjectPath, String> chunkKeyColumns = new HashMap<>();

    public MySqlSourceConfigFactory hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    /** Integer port number of the MySQL database server. */
    public MySqlSourceConfigFactory port(int port) {
        this.port = port;
        return this;
    }

    /**
     * An optional list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring. By default all
     * databases will be monitored.
     */
    public MySqlSourceConfigFactory databaseList(String... databaseList) {
        this.databaseList = Arrays.asList(databaseList);
        return this;
    }

    /**
     * An optional list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form databaseName.tableName. By default the connector will monitor
     * every non-system table in each monitored database.
     */
    public MySqlSourceConfigFactory tableList(String... tableList) {
        this.tableList = Arrays.asList(tableList);
        return this;
    }

    /** Name of the MySQL database to use when connecting to the MySQL database server. */
    public MySqlSourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    /** Password to use when connecting to the MySQL database server. */
    public MySqlSourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    /**
     * A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like
     * '5400', the numeric ID range syntax is like '5400-5408', The numeric ID range syntax is
     * required when 'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all
     * currently-running database processes in the MySQL cluster. This connector joins the MySQL
     * cluster as another server (with this unique ID) so it can read the binlog. By default, a
     * random number is generated between 5400 and 6400, though we recommend setting an explicit
     * value."
     */
    public MySqlSourceConfigFactory serverId(String serverId) {
        this.serverIdRange = ServerIdRange.from(serverId);
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in MYSQL converted to STRING. See more
     * https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types
     */
    public MySqlSourceConfigFactory serverTimeZone(String timeZone) {
        this.serverTimeZone = timeZone;
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public MySqlSourceConfigFactory chunkKeyColumn(ObjectPath objectPath, String chunkKeyColumn) {
        this.chunkKeyColumns.put(objectPath, chunkKeyColumn);
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public MySqlSourceConfigFactory chunkKeyColumn(Map<ObjectPath, String> chunkKeyColumns) {
        this.chunkKeyColumns.putAll(chunkKeyColumns);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public MySqlSourceConfigFactory splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public MySqlSourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        this.splitMetaGroupSize = splitMetaGroupSize;
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public MySqlSourceConfigFactory distributionFactorUpper(double distributionFactorUpper) {
        this.distributionFactorUpper = distributionFactorUpper;
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public MySqlSourceConfigFactory distributionFactorLower(double distributionFactorLower) {
        this.distributionFactorLower = distributionFactorLower;
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public MySqlSourceConfigFactory fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the MySQL database
     * server before timing out.
     */
    public MySqlSourceConfigFactory connectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /** The connection pool size. */
    public MySqlSourceConfigFactory connectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    /** The max retry times to get connection. */
    public MySqlSourceConfigFactory connectMaxRetries(int connectMaxRetries) {
        this.connectMaxRetries = connectMaxRetries;
        return this;
    }

    /** Whether the {@link MySqlSource} should output the schema changes or not. */
    public MySqlSourceConfigFactory includeSchemaChanges(boolean includeSchemaChanges) {
        this.includeSchemaChanges = includeSchemaChanges;
        return this;
    }

    /** Whether the {@link MySqlSource} should scan the newly added tables or not. */
    public MySqlSourceConfigFactory scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        return this;
    }

    /** Custom properties that will overwrite the default JDBC connection URL. */
    public MySqlSourceConfigFactory jdbcProperties(Properties jdbcProperties) {
        this.jdbcProperties = jdbcProperties;
        return this;
    }

    /** Specifies the startup options. */
    public MySqlSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        this.startupOptions = startupOptions;
        return this;
    }

    public MySqlSourceConfigFactory heartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    /** The Debezium MySQL connector properties. For example, "snapshot.mode". */
    public MySqlSourceConfigFactory debeziumProperties(Properties properties) {
        this.dbzProperties = properties;
        return this;
    }

    /**
     * Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more
     * https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished.
     */
    public MySqlSourceConfigFactory closeIdleReaders(boolean closeIdleReaders) {
        this.closeIdleReaders = closeIdleReaders;
        return this;
    }

    /** Creates a new {@link MySqlSourceConfig} for the given subtask {@code subtaskId}. */
    public MySqlSourceConfig createConfig(int subtaskId) {
        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the particular
        // MySQL database server/cluster being monitored. The logical name should be
        // unique across all other connectors, since it is used as a prefix for all
        // Kafka topic names emanating from this connector.
        // Only alphanumeric characters and underscores should be used.
        return createConfig(subtaskId, "mysql_binlog_source");
    }

    /** Creates a new {@link MySqlSourceConfig} for the given subtask {@code subtaskId}. */
    public MySqlSourceConfig createConfig(int subtaskId, String serverName) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        props.setProperty("database.server.name", serverName);
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.fetchSize", String.valueOf(fetchSize));
        props.setProperty("database.responseBuffering", "adaptive");
        props.setProperty("database.serverTimezone", serverTimeZone);
        // database history
        props.setProperty(
                "database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty(
                "database.history.instance.name", UUID.randomUUID().toString() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));
        props.setProperty("connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));
        // the underlying debezium reader should always capture the schema changes and forward them.
        // Note: the includeSchemaChanges parameter is used to control emitting the schema record,
        // only DataStream API program need to emit the schema record, the Table API need not
        props.setProperty("include.schema.changes", String.valueOf(true));
        // disable the offset flush totally
        props.setProperty("offset.flush.interval.ms", String.valueOf(Long.MAX_VALUE));
        // disable tombstones
        props.setProperty("tombstones.on.delete", String.valueOf(false));
        props.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatInterval.toMillis()));
        // debezium use "long" mode to handle unsigned bigint by default,
        // but it'll cause lose of precise when the value is larger than 2^63,
        // so use "precise" mode to avoid it.
        props.put("bigint.unsigned.handling.mode", "precise");

        if (serverIdRange != null) {
            int serverId = serverIdRange.getServerId(subtaskId);
            props.setProperty("database.server.id", String.valueOf(serverId));
        }
        if (databaseList != null) {
            props.setProperty("database.include.list", String.join(",", databaseList));
        }
        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }
        if (serverTimeZone != null) {
            props.setProperty("database.serverTimezone", serverTimeZone);
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        if (jdbcProperties == null) {
            jdbcProperties = new Properties();
        }

        return new MySqlSourceConfig(
                hostname,
                port,
                username,
                password,
                databaseList,
                tableList,
                serverIdRange,
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                scanNewlyAddedTableEnabled,
                closeIdleReaders,
                props,
                jdbcProperties,
                chunkKeyColumns);
    }
}
