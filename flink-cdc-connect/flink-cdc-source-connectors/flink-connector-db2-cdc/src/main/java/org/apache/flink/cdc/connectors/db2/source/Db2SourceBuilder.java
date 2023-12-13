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

package org.apache.flink.cdc.connectors.db2.source;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.offset.LsnFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class for {@link Db2IncrementalSource} to make it easier for the users to construct a
 * {@link Db2IncrementalSource}.
 *
 * <p>Check the Java docs of each individual method to learn more about the settings to build a
 * {@link Db2IncrementalSource}.
 */
@PublicEvolving
public class Db2SourceBuilder<T> {

    private final Db2SourceConfigFactory configFactory = new Db2SourceConfigFactory();

    private LsnFactory offsetFactory;

    private Db2Dialect dialect;

    private DebeziumDeserializationSchema<T> deserializer;

    /** Hostname of the Db2 database server. */
    public Db2SourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Integer port number of the Db2 database server. */
    public Db2SourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /**
     * A required list of regular expressions that match database names to be monitored; any
     * database name not included in the whitelist will be excluded from monitoring.
     */
    public Db2SourceBuilder<T> databaseList(String... databaseList) {
        this.configFactory.databaseList(databaseList);
        return this;
    }

    /**
     * A required list of regular expressions that match fully-qualified table identifiers for
     * tables to be monitored; any table not included in the list will be excluded from monitoring.
     * Each identifier is of the form {@code <schemaName>.<tableName>}.
     */
    public Db2SourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /** Name of the Db2 database to use when connecting to the Db2 database server. */
    public Db2SourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the Db2 database server. */
    public Db2SourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /**
     * The session time zone in database server, e.g. "America/Los_Angeles". It controls how the
     * TIMESTAMP type in Db2 converted to STRING. See more
     * https://debezium.io/documentation/reference/1.9/connectors/db2.html#db2-temporal-types
     */
    public Db2SourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public Db2SourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /**
     * The group size of split meta, if the meta size exceeds the group size, the meta will be will
     * be divided into multiple groups.
     */
    public Db2SourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /**
     * The upper bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public Db2SourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public Db2SourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public Db2SourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /**
     * The maximum time that the connector should wait after trying to connect to the Db2 database
     * server before timing out.
     */
    public Db2SourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The max retry times to get connection. */
    public Db2SourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /** The connection pool size. */
    public Db2SourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** Whether the {@link Db2IncrementalSource} should output the schema changes or not. */
    public Db2SourceBuilder<T> includeSchemaChanges(boolean includeSchemaChanges) {
        this.configFactory.includeSchemaChanges(includeSchemaChanges);
        return this;
    }

    /** Specifies the startup options. */
    public Db2SourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public Db2SourceBuilder<T> chunkKeyColumn(String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(chunkKeyColumn);
        return this;
    }

    /** The Debezium Db2 connector properties. For example, "snapshot.mode". */
    public Db2SourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    /**
     * The deserializer used to convert from consumed {@link
     * org.apache.kafka.connect.source.SourceRecord}.
     */
    public Db2SourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Whether to close idle readers at the end of the snapshot phase. This feature depends on
     * FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be
     * greater than or equal to 1.14, and the configuration <code>
     * 'execution.checkpointing.checkpoints-after-tasks-finish.enabled'</code> needs to be set to
     * true.
     *
     * <p>See more <a
     * href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished">FLIP-147:
     * Support Checkpoints After Tasks Finished</a>.
     */
    public Db2SourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    /**
     * Whether to skip backfill in snapshot reading phase.
     *
     * <p>If backfill is skipped, changes on captured tables during snapshot phase will be consumed
     * later in redo logs reading phase instead of being merged into the snapshot.
     *
     * <p>WARNING: Skipping backfill might lead to data inconsistency because some redo logs events
     * happened within the snapshot phase might be replayed (only at-least-once semantic is
     * promised). For example updating an already updated value in snapshot, or deleting an already
     * deleted entry in snapshot. These replayed redo logs events should be handled specially.
     */
    public Db2SourceBuilder<T> skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return this;
    }

    /**
     * Build the {@link Db2IncrementalSource}.
     *
     * @return a Db2ParallelSource with the settings made for this builder.
     */
    public Db2IncrementalSource<T> build() {
        this.offsetFactory = new LsnFactory();
        this.dialect = new Db2Dialect(configFactory.create(0));
        return new Db2IncrementalSource<T>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    /** The {@link JdbcIncrementalSource} implementation for Db2. */
    public static class Db2IncrementalSource<T> extends JdbcIncrementalSource<T> {

        public Db2IncrementalSource(
                Db2SourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                LsnFactory offsetFactory,
                Db2Dialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }

        public static <T> Db2SourceBuilder<T> builder() {
            return new Db2SourceBuilder<>();
        }
    }
}
