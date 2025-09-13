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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.reader.PostgresSourceReader;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The source builder for PostgresIncrementalSource. */
@Experimental
public class PostgresSourceBuilder<T> {

    private final PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceBuilder.class);

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
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in MYSQL converted to STRING. See more
     * https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types
     */
    public PostgresSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
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

    /** Specifies the startup options. */
    public PostgresSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public PostgresSourceBuilder<T> chunkKeyColumn(ObjectPath objectPath, String chunkKeyColumn) {
        LOG.info("Setting chunk key column for table " + objectPath + " to " + chunkKeyColumn);
        this.configFactory.chunkKeyColumn(objectPath, chunkKeyColumn);
        return this;
    }

    /** The Debezium Postgres connector properties. For example, "snapshot.mode". */
    public PostgresSourceBuilder<T> debeziumProperties(Properties properties) {
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
    public PostgresSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
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
    public PostgresSourceBuilder<T> skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return this;
    }

    /** Whether the {@link PostgresSourceEnumerator} should scan the newly added tables or not. */
    public PostgresSourceBuilder<T> scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    /**
     * Whether the {@link PostgresSourceEnumerator} should assign the unbounded chunks first or not
     * during snapshot reading phase.
     */
    public PostgresSourceBuilder<T> assignUnboundedChunkFirst(boolean assignUnboundedChunkFirst) {
        this.configFactory.assignUnboundedChunkFirst(assignUnboundedChunkFirst);
        return this;
    }

    /** Set the {@code LSN} checkpoints delay number for Postgres to commit the offsets. */
    public PostgresSourceBuilder<T> lsnCommitCheckpointsDelay(int lsnCommitDelay) {
        this.configFactory.setLsnCommitCheckpointsDelay(lsnCommitDelay);
        return this;
    }

    /**
     * Build the {@link PostgresIncrementalSource}.
     *
     * @return a PostgresParallelSource with the settings made for this builder.
     */
    public PostgresIncrementalSource<T> build() {
        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        // get params of jdbcsourceconfig

        Constructor<?>[] constructors = JdbcSourceConfig.class.getDeclaredConstructors();
        for (Constructor<?> constructor : constructors) {
            // Get params type of constructor
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            LOG.info("Constructor: " + constructor + ", parameter types: ");
            for (Class<?> paramType : parameterTypes) {
                LOG.info(" \n " + paramType.getName());
            }
        }
        PostgresDialect dialect = new PostgresDialect(configFactory.create(0));

        PostgresIncrementalSource<T> source =
                new PostgresIncrementalSource<>(
                        configFactory, checkNotNull(deserializer), offsetFactory, dialect);

        return source;
    }

    public PostgresSourceConfigFactory getConfigFactory() {
        return configFactory;
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
        public PostgresSourceEnumerator createEnumerator(
                SplitEnumeratorContext<SourceSplitBase> enumContext) {
            final SplitAssigner splitAssigner;
            PostgresSourceConfig sourceConfig = (PostgresSourceConfig) configFactory.create(0);
            if (!sourceConfig.getStartupOptions().isStreamOnly()) {
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
                                    offsetFactory,
                                    enumContext);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Failed to discover captured tables for enumerator", e);
                }
            } else {
                splitAssigner =
                        new StreamSplitAssigner(
                                sourceConfig, dataSourceDialect, offsetFactory, enumContext);
            }

            return new PostgresSourceEnumerator(
                    enumContext,
                    sourceConfig,
                    splitAssigner,
                    (PostgresDialect) dataSourceDialect,
                    this.getBoundedness());
        }

        @Override
        public PostgresSourceEnumerator restoreEnumerator(
                SplitEnumeratorContext<SourceSplitBase> enumContext,
                PendingSplitsState checkpoint) {
            final SplitAssigner splitAssigner;
            PostgresSourceConfig sourceConfig = (PostgresSourceConfig) configFactory.create(0);
            if (checkpoint instanceof HybridPendingSplitsState) {
                splitAssigner =
                        new HybridSplitAssigner<>(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                (HybridPendingSplitsState) checkpoint,
                                dataSourceDialect,
                                offsetFactory,
                                enumContext);
            } else if (checkpoint instanceof StreamPendingSplitsState) {
                splitAssigner =
                        new StreamSplitAssigner(
                                sourceConfig,
                                (StreamPendingSplitsState) checkpoint,
                                dataSourceDialect,
                                offsetFactory,
                                enumContext);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported restored PendingSplitsState: " + checkpoint);
            }

            return new PostgresSourceEnumerator(
                    enumContext,
                    sourceConfig,
                    splitAssigner,
                    (PostgresDialect) dataSourceDialect,
                    getBoundedness());
        }

        @Override
        public PostgresSourceReader createReader(SourceReaderContext readerContext)
                throws Exception {
            // create source config for the given subtask (e.g. unique server id)
            PostgresSourceConfig sourceConfig =
                    (PostgresSourceConfig) configFactory.create(readerContext.getIndexOfSubtask());
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                    new FutureCompletingBlockingQueue<>();

            final SourceReaderMetrics sourceReaderMetrics =
                    new SourceReaderMetrics(readerContext.metricGroup());

            IncrementalSourceReaderContext incrementalSourceReaderContext =
                    new IncrementalSourceReaderContext(readerContext);
            Supplier<IncrementalSourceSplitReader<JdbcSourceConfig>> splitReaderSupplier =
                    () ->
                            new IncrementalSourceSplitReader<>(
                                    readerContext.getIndexOfSubtask(),
                                    dataSourceDialect,
                                    sourceConfig,
                                    incrementalSourceReaderContext,
                                    snapshotHooks);
            return new PostgresSourceReader(
                    elementsQueue,
                    splitReaderSupplier,
                    createRecordEmitter(sourceConfig, sourceReaderMetrics),
                    readerContext.getConfiguration(),
                    incrementalSourceReaderContext,
                    sourceConfig,
                    sourceSplitSerializer,
                    dataSourceDialect);
        }

        public static <T> PostgresSourceBuilder<T> builder() {
            return new PostgresSourceBuilder<>();
        }
    }
}
