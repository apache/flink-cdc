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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The source builder for GaussDBIncrementalSource. */
@Experimental
public class GaussDBSourceBuilder<T> {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBSourceBuilder.class);
    private final GaussDBSourceConfigFactory configFactory = new GaussDBSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    private GaussDBSourceBuilder() {}

    /**
     * The name of the GaussDB logical decoding plug-in installed on the server. Supported value is
     * mppdb_decoding.
     */
    public GaussDBSourceBuilder<T> decodingPluginName(String name) {
        this.configFactory.decodingPluginName(name);
        return this;
    }

    /** The hostname of the database to monitor for changes. */
    public GaussDBSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Integer port number of the GaussDB database server. */
    public GaussDBSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /** The name of the GaussDB database from which to stream the changes. */
    public GaussDBSourceBuilder<T> database(String database) {
        this.configFactory.databaseList(database);
        return this;
    }

    /**
     * An required list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring.
     */
    public GaussDBSourceBuilder<T> schemaList(String... schemaList) {
        this.configFactory.schemaList(schemaList);
        return this;
    }

    /**
     * An required list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form {@code <schemaName>.<tableName>}.
     */
    public GaussDBSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /** Name of the GaussDB database to use when connecting to the GaussDB database server. */
    public GaussDBSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the GaussDB database server. */
    public GaussDBSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /**
     * The name of the GaussDB logical decoding slot that was created for streaming changes from a
     * particular plug-in for a particular database/schema. The server uses this slot to stream
     * events to the connector that you are configuring. Default is "flink".
     *
     * <p>Slot names must conform to GaussDB replication slot naming rules, which state: "Each
     * replication slot has a name, which can contain lower-case letters, numbers, and the
     * underscore character."
     */
    public GaussDBSourceBuilder<T> slotName(String slotName) {
        this.configFactory.slotName(slotName);
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in GaussDB converted to STRING.
     */
    public GaussDBSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public GaussDBSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be
     * divided into multiple groups.
     */
    public GaussDBSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public GaussDBSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public GaussDBSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public GaussDBSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the GaussDB
     * database server before timing out.
     */
    public GaussDBSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The max retry times to get connection. */
    public GaussDBSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /** The connection pool size. */
    public GaussDBSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** Specifies the startup options. */
    public GaussDBSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public GaussDBSourceBuilder<T> chunkKeyColumn(String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(chunkKeyColumn);
        return this;
    }

    /** The Debezium GaussDB connector properties. For example, "snapshot.mode". */
    public GaussDBSourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    /**
     * scan.incremental.close-idle-reader.enabled
     *
     * <p>Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more
     * https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished.
     */
    public GaussDBSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public GaussDBSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /** The heartbeat interval for the GaussDB server. */
    public GaussDBSourceBuilder<T> heartbeatInterval(Duration heartbeatInterval) {
        this.configFactory.heartbeatInterval(heartbeatInterval);
        return this;
    }

    /**
     * Whether to skip backfill in snapshot reading phase.
     *
     * <p>If backfill is skipped, changes on captured tables during snapshot phase will be consumed
     * later in stream reading phase instead of being merged into the snapshot.
     *
     * <p>WARNING: Skipping backfill might lead to data inconsistency because some stream events
     * happened within the snapshot phase might be replayed (only at-least-once semantic is
     * promised). For example updating an already updated value in snapshot, or deleting an already
     * deleted entry in snapshot. These replayed stream events should be handled specially.
     */
    public GaussDBSourceBuilder<T> skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return this;
    }

    /** Whether the source should scan the newly added tables or not. */
    public GaussDBSourceBuilder<T> scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    /**
     * Whether the source should assign the unbounded chunks first or not during snapshot reading
     * phase.
     */
    public GaussDBSourceBuilder<T> assignUnboundedChunkFirst(boolean assignUnboundedChunkFirst) {
        this.configFactory.assignUnboundedChunkFirst(assignUnboundedChunkFirst);
        return this;
    }

    /**
     * Build the {@link GaussDBIncrementalSource}.
     *
     * @return a GaussDBIncrementalSource with the settings made for this builder.
     */
    public GaussDBIncrementalSource<T> build() {
        LOG.info("=== GaussDBSourceBuilder.build() STARTED ===");
        LOG.info("Building GaussDBIncrementalSource with config factory: {}", configFactory);
        GaussDBOffsetFactory offsetFactory = new GaussDBOffsetFactory();
        LOG.info("Created GaussDBOffsetFactory");
        GaussDBDialect dialect = new GaussDBDialect(configFactory.create(0));
        LOG.info("Created GaussDBDialect");
        GaussDBIncrementalSource<T> source =
                new GaussDBIncrementalSource<>(
                        configFactory, checkNotNull(deserializer), offsetFactory, dialect);
        LOG.info("=== GaussDBIncrementalSource created successfully ===");
        return source;
    }

    public GaussDBSourceConfigFactory getConfigFactory() {
        return configFactory;
    }

    /** The GaussDB source based on the incremental snapshot framework. */
    @Experimental
    public static class GaussDBIncrementalSource<T> extends JdbcIncrementalSource<T> {
        public GaussDBIncrementalSource(
                GaussDBSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                GaussDBOffsetFactory offsetFactory,
                GaussDBDialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }

        public static <T> GaussDBSourceBuilder<T> builder() {
            return new GaussDBSourceBuilder<>();
        }
    }
}
