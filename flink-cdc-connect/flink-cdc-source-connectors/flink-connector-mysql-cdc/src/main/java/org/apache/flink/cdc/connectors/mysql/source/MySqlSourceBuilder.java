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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.table.catalog.ObjectPath;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link MySqlSource} to make it easier for the users to construct a {@link
 * MySqlSource}.
 *
 * <pre>{@code
 * MySqlSource
 *     .<String>builder()
 *     .hostname("localhost")
 *     .port(3306)
 *     .databaseList("mydb")
 *     .tableList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .serverId(5400)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>Check the Java docs of each individual method to learn more about the settings to build a
 * {@link MySqlSource}.
 */
@PublicEvolving
public class MySqlSourceBuilder<T> {
    private final MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    public MySqlSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Integer port number of the MySQL database server. */
    public MySqlSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /**
     * An required list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring.
     */
    public MySqlSourceBuilder<T> databaseList(String... databaseList) {
        this.configFactory.databaseList(databaseList);
        return this;
    }

    /**
     * An required list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form {@code <databaseName>.<tableName>}.
     */
    public MySqlSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /** Name of the MySQL database to use when connecting to the MySQL database server. */
    public MySqlSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the MySQL database server. */
    public MySqlSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
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
    public MySqlSourceBuilder<T> serverId(String serverId) {
        this.configFactory.serverId(serverId);
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in MYSQL converted to STRING. See more
     * https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types
     */
    public MySqlSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public MySqlSourceBuilder<T> chunkKeyColumn(ObjectPath objectPath, String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(objectPath, chunkKeyColumn);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public MySqlSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public MySqlSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public MySqlSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public MySqlSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public MySqlSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the MySQL database
     * server before timing out.
     */
    public MySqlSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The max retry times to get connection. */
    public MySqlSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /** The connection pool size. */
    public MySqlSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** Whether the {@link MySqlSource} should output the schema changes or not. */
    public MySqlSourceBuilder<T> includeSchemaChanges(boolean includeSchemaChanges) {
        this.configFactory.includeSchemaChanges(includeSchemaChanges);
        return this;
    }

    /** Whether the {@link MySqlSource} should scan the newly added tables or not. */
    public MySqlSourceBuilder<T> scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    /** Specifies the startup options. */
    public MySqlSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /** Custom properties that will overwrite the default JDBC connection URL. */
    public MySqlSourceBuilder<T> jdbcProperties(Properties jdbcProperties) {
        this.configFactory.jdbcProperties(jdbcProperties);
        return this;
    }

    /** The Debezium MySQL connector properties. For example, "snapshot.mode". */
    public MySqlSourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public MySqlSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /** The interval of heartbeat event. */
    public MySqlSourceBuilder<T> heartbeatInterval(Duration heartbeatInterval) {
        this.configFactory.heartbeatInterval(heartbeatInterval);
        return this;
    }

    /**
     * Whether to skip backfill in snapshot reading phase.
     *
     * <p>If backfill is skipped, changes on captured tables during snapshot phase will be consumed
     * later in binlog reading phase instead of being merged into the snapshot.
     *
     * <p>WARNING: Skipping backfill might lead to data inconsistency because some binlog events
     * happened within the snapshot phase might be replayed (only at-least-once semantic is
     * promised). For example updating an already updated value in snapshot, or deleting an already
     * deleted entry in snapshot. These replayed binlog events should be handled specially.
     */
    public MySqlSourceBuilder<T> skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return this;
    }

    /**
     * Whether to use legacy json format. The default value is true, which means there is no
     * whitespace before value and after comma in json format.
     */
    public MySqlSourceBuilder<T> useLegacyJsonFormat(boolean useLegacyJsonFormat) {
        this.configFactory.useLegacyJsonFormat(useLegacyJsonFormat);
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
    public MySqlSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    /** Whether to parse gh-ost utility generated schema change events. Defaults to false. */
    public MySqlSourceBuilder<T> parseOnLineSchemaChanges(boolean parseOnLineSchemaChanges) {
        this.configFactory.parseOnLineSchemaChanges(parseOnLineSchemaChanges);
        return this;
    }

    /**
     * Whether the {@link MySqlSource} should assign the unbounded chunks first or not during
     * snapshot reading phase.
     */
    public MySqlSourceBuilder<T> assignUnboundedChunkFirst(boolean assignUnboundedChunkFirst) {
        this.configFactory.assignUnboundedChunkFirst(assignUnboundedChunkFirst);
        return this;
    }

    /**
     * Build the {@link MySqlSource}.
     *
     * @return a MySqlParallelSource with the settings made for this builder.
     */
    public MySqlSource<T> build() {
        return new MySqlSource<>(configFactory, checkNotNull(deserializer));
    }
}
