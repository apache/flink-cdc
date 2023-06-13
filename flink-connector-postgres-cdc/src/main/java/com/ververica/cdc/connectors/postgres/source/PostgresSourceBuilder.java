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

package com.ververica.cdc.connectors.postgres.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.options.StartupMode;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import com.ververica.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator;
import com.ververica.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.relational.TableId;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The source builder for PostgresIncrementalSource. */
@Experimental
public class PostgresSourceBuilder<T> {

    private final PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    private PostgresSourceBuilder() {}

    /**
     * The name of the Postgres logical decoding plug-in installed on the server. Supported values
     * are decoderbufs, wal2json, wal2json_rds, wal2json_streaming, wal2json_rds_streaming and
     * pgoutput.
     */
    public PostgresSourceBuilder<T> decodingPluginName(String name) {
        this.configFactory.decodingPluginName(name);
        return this;
    }

    /** The hostname of the database to monitor for changes. */
    public PostgresSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Integer port number of the Postgres database server. */
    public PostgresSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /** The name of the PostgreSQL database from which to stream the changes. */
    public PostgresSourceBuilder<T> database(String database) {
        this.configFactory.database(database);
        return this;
    }

    /**
     * An required list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring.
     */
    public PostgresSourceBuilder<T> schemaList(String... schemaList) {
        this.configFactory.schemaList(schemaList);
        return this;
    }

    /**
     * An required list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form {@code <schemaName>.<tableName>}.
     */
    public PostgresSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /** Name of the Postgres database to use when connecting to the Postgres database server. */
    public PostgresSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the Postgres database server. */
    public PostgresSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /**
     * The name of the PostgreSQL logical decoding slot that was created for streaming changes from
     * a particular plug-in for a particular database/schema. The server uses this slot to stream
     * events to the connector that you are configuring. Default is "flink".
     *
     * <p>Slot names must conform to <a
     * href="https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS-MANIPULATION">PostgreSQL
     * replication slot naming rules</a>, which state: "Each replication slot has a name, which can
     * contain lower-case letters, numbers, and the underscore character."
     */
    public PostgresSourceBuilder<T> slotName(String slotName) {
        this.configFactory.slotName(slotName);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public PostgresSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public PostgresSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public PostgresSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public PostgresSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public PostgresSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the Postgres
     * database server before timing out.
     */
    public PostgresSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The max retry times to get connection. */
    public PostgresSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /** The connection pool size. */
    public PostgresSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** Whether the {@link PostgresIncrementalSource} should output the schema changes or not. */
    public PostgresSourceBuilder<T> includeSchemaChanges(boolean includeSchemaChanges) {
        this.configFactory.includeSchemaChanges(includeSchemaChanges);
        return this;
    }

    /** Specifies the startup options. */
    public PostgresSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public PostgresSourceBuilder<T> chunkKeyColumn(String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(chunkKeyColumn);
        return this;
    }

    /** The Debezium Postgres connector properties. For example, "snapshot.mode". */
    public PostgresSourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public PostgresSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /** The heartbeat interval for the Postgres server. */
    public PostgresSourceBuilder<T> heartbeatInterval(Duration heartbeatInterval) {
        this.configFactory.heartbeatInterval(heartbeatInterval);
        return this;
    }

    /**
     * Build the {@link PostgresIncrementalSource}.
     *
     * @return a PostgresParallelSource with the settings made for this builder.
     */
    public PostgresIncrementalSource<T> build() {
        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        PostgresDialect dialect = new PostgresDialect(configFactory);
        return new PostgresIncrementalSource<>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    /** The Postgres source based on the incremental snapshot framework. */
    @Experimental
    public static class PostgresIncrementalSource<T> extends JdbcIncrementalSource<T> {
        public PostgresIncrementalSource(
                PostgresSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                PostgresOffsetFactory offsetFactory,
                PostgresDialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }

        @Override
        public SplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
                SplitEnumeratorContext<SourceSplitBase> enumContext) {
            final SplitAssigner splitAssigner;
            PostgresSourceConfig sourceConfig = (PostgresSourceConfig) configFactory.create(0);
            if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
                try {
                    final List<TableId> remainingTables =
                            dataSourceDialect.discoverDataCollections(sourceConfig);
                    boolean isTableIdCaseSensitive =
                            dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                    splitAssigner =
                            new HybridSplitAssigner<>(
                                    sourceConfig,
                                    enumContext.currentParallelism(),
                                    remainingTables,
                                    isTableIdCaseSensitive,
                                    dataSourceDialect,
                                    offsetFactory);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Failed to discover captured tables for enumerator", e);
                }
            } else {
                splitAssigner =
                        new StreamSplitAssigner(sourceConfig, dataSourceDialect, offsetFactory);
            }

            return new PostgresSourceEnumerator(
                    enumContext, sourceConfig, splitAssigner, (PostgresDialect) dataSourceDialect);
        }

        public static <T> PostgresSourceBuilder<T> builder() {
            return new PostgresSourceBuilder<>();
        }
    }
}
