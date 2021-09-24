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

import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_SERVER_NAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_WHITE_LIST;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_ID;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.TABLE_WHITE_LIST;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Builder class of {@link MySqlParallelSource}. */
public class MySqlParallelSourceBuilder<T> {
    private int port = 3306; // default 3306 port
    private String hostname;
    private String[] databaseList;
    private String username;
    private String password;
    private String serverId;
    private String serverTimeZone;
    private String[] tableList;
    private int splitSize;
    private int fetchSize;
    private Duration connectTimeout;
    private StartupOptions startupOptions;
    private Properties dbzProperties;
    private DebeziumDeserializationSchema<T> deserializer;

    public static <T> MySqlParallelSourceBuilder<T> builder() {
        return new MySqlParallelSourceBuilder<>();
    }

    public MySqlParallelSourceBuilder<T> hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    /** Integer port number of the MySQL database server. */
    public MySqlParallelSourceBuilder<T> port(int port) {
        this.port = port;
        return this;
    }

    /**
     * An optional list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring. By default all
     * databases will be monitored.
     */
    public MySqlParallelSourceBuilder<T> databaseList(String... databaseList) {
        this.databaseList = databaseList;
        return this;
    }

    /**
     * An optional list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form databaseName.tableName. By default the connector will monitor
     * every non-system table in each monitored database.
     */
    public MySqlParallelSourceBuilder<T> tableList(String... tableList) {
        this.tableList = tableList;
        return this;
    }

    /** Name of the MySQL database to use when connecting to the MySQL database server. */
    public MySqlParallelSourceBuilder<T> username(String username) {
        this.username = username;
        return this;
    }

    /** Password to use when connecting to the MySQL database server. */
    public MySqlParallelSourceBuilder<T> password(String password) {
        this.password = password;
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in MYSQL converted to STRING. See more
     * https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-temporal-types
     */
    public MySqlParallelSourceBuilder<T> serverTimeZone(String serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
        return this;
    }

    /**
     * "A numeric ID or a numeric ID range of this database client, " + "The numeric ID syntax is
     * like '5400', the numeric ID range syntax " + "is like '5400-5408', The numeric ID range
     * syntax is recommended when " + "'scan.incremental.snapshot.enabled' enabled. Every ID must be
     * unique across all " + "currently-running database processes in the MySQL cluster. This
     * connector" + " joins the MySQL cluster as another server (with this unique ID) " + "so it can
     * read the binlog. By default, a random number is generated between" + " 5400 and 6400, though
     * we recommend setting an explicit value."
     */
    public MySqlParallelSourceBuilder<T> serverId(String serverId) {
        this.serverId = serverId;
        return this;
    }

    /**
     * The chunk size (number of rows) of table snapshot, captured tables are split into multiple
     * chunks when read the snapshot of table.
     */
    public MySqlParallelSourceBuilder<T> splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public MySqlParallelSourceBuilder<T> fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the MySQL database
     * server before timing out.
     */
    public MySqlParallelSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /** Specifies the startup options. */
    public MySqlParallelSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.startupOptions = startupOptions;
        return this;
    }

    /** The Debezium MySQL connector properties. For example, "snapshot.mode". */
    public MySqlParallelSourceBuilder<T> debeziumProperties(Properties properties) {
        this.dbzProperties = properties;
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public MySqlParallelSourceBuilder<T> deserializer(
            DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public MySqlParallelSource<T> build() {
        Configuration flinkConf = getFlinkConf();
        Configuration dbzConf = getDbzConf();
        return new MySqlParallelSource<>(deserializer, flinkConf, dbzConf);
    }

    private Configuration getFlinkConf() {
        Map<String, String> properties = new HashMap<>();
        if (serverId != null) {
            properties.put(SERVER_ID.key(), serverId);
        }

        checkState(
                splitSize > 1,
                String.format(
                        "The value of option '%s' must larger than 1, but is %d",
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key(), splitSize));

        properties.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key(), String.valueOf(splitSize));
        // set mode
        switch (startupOptions.startupMode) {
            case INITIAL:
                properties.put("scan.startup.mode", "initial");
                break;

            case LATEST_OFFSET:
                properties.put("scan.startup.mode", "latest-offset");
                break;

            default:
                throw new UnsupportedOperationException();
        }
        return Configuration.fromMap(properties);
    }

    private Configuration getDbzConf() {
        Map<String, String> properties = new HashMap<>();
        if (dbzProperties != null) {
            dbzProperties.forEach((k, v) -> properties.put(k.toString(), v.toString()));
        }

        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());

        properties.put("database.hostname", checkNotNull(hostname));
        properties.put("database.user", checkNotNull(username));
        properties.put("database.password", checkNotNull(password));
        properties.put("database.port", String.valueOf(port));

        properties.put("database.history.skip.unparseable.ddl", String.valueOf(true));
        properties.put("database.server.name", DATABASE_SERVER_NAME);
        properties.put("connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));

        if (databaseList != null) {
            properties.put(DATABASE_WHITE_LIST, String.join(",", databaseList));
        }
        if (tableList != null) {
            properties.put(TABLE_WHITE_LIST, String.join(",", tableList));
        }

        if (serverTimeZone != null) {
            properties.put("database.serverTimezone", serverTimeZone);
        }

        properties.put("database.responseBuffering", "adaptive");
        properties.put("database.history.prefer.ddl", String.valueOf(true));
        properties.put("tombstones.on.delete", String.valueOf(false));
        properties.put("database.fetchSize", String.valueOf(fetchSize));

        return Configuration.fromMap(properties);
    }
}
