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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.ververica.cdc.connectors.mysql.MySqlValidator;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceConfig.DATABASE_SERVER_ID;
import static com.ververica.cdc.connectors.mysql.source.MySqlParallelSourceConfig.getServerIdForSubTask;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The MySQL CDC Source based on FLIP-27 and Watermark Signal Algorithm which supports parallel
 * reading snapshot of table and then continue to capture data change from binlog.
 *
 * <pre>
 *     1. The source supports parallel capturing table change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source does need apply any lock of MySQL.
 * </pre>
 *
 * @param <T> The record type.
 */
@Internal
public class MySqlParallelSource<T>
        implements Source<T, MySqlSplit, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final MySqlParallelSourceConfig sourceConfig;
    private final DebeziumDeserializationSchema<T> deserializationSchema;

    private MySqlParallelSource(
            MySqlParallelSourceConfig sourceConfig,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        this.sourceConfig = sourceConfig;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, MySqlSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        final MySqlParallelSourceConfig readerConfig =
                getReaderConfig(readerContext.getIndexOfSubtask());
        final MySqlSourceReaderMetrics sourceReaderMetrics =
                new MySqlSourceReaderMetrics(readerContext.metricGroup());
        sourceReaderMetrics.registerMetrics();
        Supplier<MySqlSplitReader> splitReaderSupplier =
                () -> new MySqlSplitReader(readerConfig, readerContext.getIndexOfSubtask());
        return new MySqlSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new MySqlRecordEmitter<>(
                        deserializationSchema,
                        sourceReaderMetrics,
                        sourceConfig.includeSchemaChanges()),
                readerContext.getConfiguration(),
                readerContext);
    }

    private MySqlParallelSourceConfig getReaderConfig(int indexOfSubtask) {
        Properties readerProps = (Properties) sourceConfig.getDbzProperties().clone();
        // set the server id for each reader, will be used by debezium reader
        final Optional<String> serverId =
                getServerIdForSubTask(sourceConfig.getServerIdRange(), indexOfSubtask);
        serverId.ifPresent(s -> readerProps.setProperty(DATABASE_SERVER_ID, s));

        // set the database history instance name, it will be used by debeizum reader
        final String historyInstanceName = readerProps.getProperty(DATABASE_HISTORY_INSTANCE_NAME);
        readerProps.setProperty(
                DATABASE_HISTORY_INSTANCE_NAME, historyInstanceName + "_" + indexOfSubtask);

        // set the DatabaseHistory name for each reader, will used by debezium reader
        return new MySqlParallelSourceConfig.Builder()
                .startupMode(sourceConfig.getStartupMode())
                .splitSize(sourceConfig.getSplitSize())
                .serverIdRange(sourceConfig.getServerIdRange())
                .dbzProperties(readerProps)
                .build();
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext) {
        MySqlValidator validator = new MySqlValidator(sourceConfig.getDbzProperties());
        final int currentParallelism = enumContext.currentParallelism();

        final MySqlSplitAssigner splitAssigner =
                MySqlSourceOptions.SCAN_STARTUP_MODE
                                .defaultValue()
                                .equals(sourceConfig.getStartupMode())
                        ? new MySqlHybridSplitAssigner(sourceConfig, currentParallelism)
                        : new MySqlBinlogSplitAssigner(sourceConfig);

        return new MySqlSourceEnumerator(enumContext, splitAssigner, validator);
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext, PendingSplitsState checkpoint) {
        MySqlValidator validator = new MySqlValidator(sourceConfig.getDbzProperties());
        final MySqlSplitAssigner splitAssigner;
        final int currentParallelism = enumContext.currentParallelism();
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new MySqlHybridSplitAssigner(
                            sourceConfig,
                            currentParallelism,
                            (HybridPendingSplitsState) checkpoint);
        } else if (checkpoint instanceof BinlogPendingSplitsState) {
            splitAssigner =
                    new MySqlBinlogSplitAssigner(
                            sourceConfig, (BinlogPendingSplitsState) checkpoint);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }

        return new MySqlSourceEnumerator(enumContext, splitAssigner, validator);
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return MySqlSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsStateSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link MySqlParallelSource}. */
    public static final class Builder<T> {
        // parameter with default value
        private int port = 3306;
        private int splitSize = SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue();
        private int fetchSize = SCAN_SNAPSHOT_FETCH_SIZE.defaultValue();
        private StartupOptions startupOptions = StartupOptions.initial();
        private String serverTimeZone = SERVER_TIME_ZONE.defaultValue();
        private Duration connectTimeout = MySqlSourceOptions.CONNECT_TIMEOUT.defaultValue();
        private boolean includeSchemaChanges = false;
        private Properties dbzProperties = new Properties();

        // parameter without default value
        private String hostname;
        private String[] databaseList;
        private String username;
        private String password;
        private String serverIdRange;
        private String[] tableList;
        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /** Integer port number of the MySQL database server. */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        /**
         * An optional list of regular expressions that match database names to be monitored; any
         * database name not included in the whitelist will be excluded from monitoring. By default
         * all databases will be monitored.
         */
        public Builder<T> databaseList(String... databaseList) {
            this.databaseList = databaseList;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers for
         * tables to be monitored; any table not included in the list will be excluded from
         * monitoring. Each identifier is of the form databaseName.tableName. By default the
         * connector will monitor every non-system table in each monitored database.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        /** Name of the MySQL database to use when connecting to the MySQL database server. */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to use when connecting to the MySQL database server. */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /**
         * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
         * TIMESTAMP type in MYSQL converted to STRING. See more
         * https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-temporal-types
         */
        public Builder<T> serverTimeZone(String timeZone) {
            this.serverTimeZone = timeZone;
            return this;
        }

        /**
         * A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like
         * '5400', the numeric ID range syntax is like '5400-5408', The numeric ID range syntax is
         * required when 'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across
         * all currently-running database processes in the MySQL cluster. This connector joins the
         * MySQL cluster as another server (with this unique ID) so it can read the binlog. By
         * default, a random number is generated between 5400 and 6400, though we recommend setting
         * an explicit value."
         */
        public Builder<T> serverId(String serverId) {
            this.serverIdRange = serverId;
            return this;
        }

        /**
         * The split size (number of rows) of table snapshot, captured tables are split into
         * multiple splits when read the snapshot of table.
         */
        public Builder<T> splitSize(int splitSize) {
            this.splitSize = splitSize;
            return this;
        }

        /** The maximum fetch size for per poll when read table snapshot. */
        public Builder<T> fetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        /**
         * The maximum time that the connector should wait after trying to connect to the MySQL
         * database server before timing out.
         */
        public Builder<T> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /** Whether the {@link MySqlParallelSource} should output the schema changes or not. */
        public Builder<T> includeSchemaChanges(boolean includeSchemaChanges) {
            this.includeSchemaChanges = includeSchemaChanges;
            return this;
        }

        /** The Debezium MySQL connector props. */
        public Builder<T> debeziumProperties(Properties props) {
            this.dbzProperties.putAll(props);
            return this;
        }

        /**
         * The deserializer used to convert from consumed {@link
         * org.apache.kafka.connect.source.SourceRecord}.
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        /** Specifies the startup options. */
        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        public MySqlParallelSource<T> build() {
            final String startupMode;
            switch (startupOptions.startupMode) {
                case INITIAL:
                    startupMode = "initial";
                    dbzProperties.put("scan.startup.mode", startupMode);
                    break;
                case LATEST_OFFSET:
                    startupMode = "latest-offset";
                    dbzProperties.put("scan.startup.mode", startupMode);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            final String capturedDatabases =
                    databaseList == null ? null : String.join(",", databaseList);
            final String capturedTables = tableList == null ? null : String.join(",", tableList);

            final MySqlParallelSourceConfig parallelSourceConfig =
                    new MySqlParallelSourceConfig.Builder()
                            .startupMode(startupMode)
                            .includeSchemaChanges(includeSchemaChanges)
                            .serverIdRange(serverIdRange)
                            .splitSize(splitSize)
                            .capturedDatabases(capturedDatabases)
                            .capturedTables(capturedTables)
                            .hostname(checkNotNull(hostname))
                            .username(checkNotNull(username))
                            .password(checkNotNull(password))
                            .port(port)
                            .connectTimeout(connectTimeout)
                            .fetchSize(fetchSize)
                            .serverTimeZone(serverTimeZone)
                            .dbzProperties(dbzProperties)
                            .build();

            return new MySqlParallelSource<>(parallelSourceConfig, deserializer);
        }
    }
}
