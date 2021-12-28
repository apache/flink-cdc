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

package com.ververica.cdc.connectors.base.source.config;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.ververica.cdc.connectors.base.source.config.SourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.SERVER_TIME_ZONE;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.ververica.cdc.connectors.base.source.config.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;

/** A factory to initialize {@link SourceConfig}. */
@Internal
public abstract class SourceConfigFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    protected int port;
    protected String hostname;
    protected String username;
    protected String password;
    protected List<String> databaseList;
    protected List<String> tableList;
    protected StartupOptions startupOptions = StartupOptions.initial();
    protected int splitSize = SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue();
    protected int splitMetaGroupSize = CHUNK_META_GROUP_SIZE.defaultValue();
    protected int fetchSize = SCAN_SNAPSHOT_FETCH_SIZE.defaultValue();
    protected String serverTimeZone = SERVER_TIME_ZONE.defaultValue();
    protected Duration connectTimeout = CONNECT_TIMEOUT.defaultValue();
    protected int connectMaxRetries = CONNECT_MAX_RETRIES.defaultValue();
    protected int connectionPoolSize = CONNECTION_POOL_SIZE.defaultValue();
    protected double distributionFactorUpper =
            SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue();
    protected double distributionFactorLower =
            SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue();
    protected boolean includeSchemaChanges = false;
    protected Properties dbzProperties;

    /** Integer port number of the database server. */
    public SourceConfigFactory hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    /** Integer port number of the database server. */
    public SourceConfigFactory port(int port) {
        this.port = port;
        return this;
    }

    /**
     * An optional list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring. By default all
     * databases will be monitored.
     */
    public SourceConfigFactory databaseList(String... databaseList) {
        this.databaseList = Arrays.asList(databaseList);
        return this;
    }

    /**
     * An optional list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form databaseName.tableName. By default the connector will monitor
     * every non-system table in each monitored database.
     */
    public SourceConfigFactory tableList(String... tableList) {
        this.tableList = Arrays.asList(tableList);
        return this;
    }

    /** Name of the user to use when connecting to the database server. */
    public SourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    /** Password to use when connecting to the database server. */
    public SourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in MYSQL converted to STRING. See more
     * https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-temporal-types
     */
    public SourceConfigFactory serverTimeZone(String timeZone) {
        this.serverTimeZone = timeZone;
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public SourceConfigFactory splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be will
     * be divided into multiple groups.
     */
    public SourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        this.splitMetaGroupSize = splitMetaGroupSize;
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public SourceConfigFactory distributionFactorUpper(double distributionFactorUpper) {
        this.distributionFactorUpper = distributionFactorUpper;
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public SourceConfigFactory distributionFactorLower(double distributionFactorLower) {
        this.distributionFactorLower = distributionFactorLower;
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public SourceConfigFactory fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the MySQL database
     * server before timing out.
     */
    public SourceConfigFactory connectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /** The connection pool size. */
    public SourceConfigFactory connectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    /** The max retry times to get connection. */
    public SourceConfigFactory connectMaxRetries(int connectMaxRetries) {
        this.connectMaxRetries = connectMaxRetries;
        return this;
    }

    /** Whether the {@link SourceConfig} should output the schema changes or not. */
    public SourceConfigFactory includeSchemaChanges(boolean includeSchemaChanges) {
        this.includeSchemaChanges = includeSchemaChanges;
        return this;
    }

    /** The Debezium MySQL connector properties. For example, "snapshot.mode". */
    public SourceConfigFactory debeziumProperties(Properties properties) {
        this.dbzProperties = properties;
        return this;
    }

    /** Specifies the startup options. */
    public SourceConfigFactory startupOptions(StartupOptions startupOptions) {
        switch (startupOptions.startupMode) {
            case INITIAL:
            case LATEST_OFFSET:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported startup mode: " + startupOptions.startupMode);
        }
        this.startupOptions = startupOptions;
        return this;
    }

    /** Creates a new {@link SourceConfig} for the given subtask {@code subtaskId}. */
    public abstract SourceConfig createConfig(int subtaskId);
}
