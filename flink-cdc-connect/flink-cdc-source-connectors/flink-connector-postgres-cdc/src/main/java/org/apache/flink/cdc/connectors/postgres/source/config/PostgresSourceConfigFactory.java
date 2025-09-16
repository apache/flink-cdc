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

package org.apache.flink.cdc.connectors.postgres.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory to create Configuration for Postgres source. */
public class PostgresSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private Duration heartbeatInterval = PostgresSourceOptions.HEARTBEAT_INTERVAL.defaultValue();

    private static final String JDBC_DRIVER = "org.postgresql.Driver";

    private String pluginName = "decoderbufs";

    private String slotName = "flink";

    private String database;

    private List<String> schemaList;

    private int lsnCommitCheckpointsDelay;

    private Map<ObjectPath, String> chunkKeyColumns = new HashMap<>();

    /** Creates a new {@link PostgresSourceConfig} for the given subtask {@code subtaskId}. */
    @Override
    public PostgresSourceConfig create(int subtaskId) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        props.setProperty("connector.class", PostgresConnector.class.getCanonicalName());
        props.setProperty("plugin.name", pluginName);
        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the particular PostgreSQL
        // database server/cluster being monitored. The logical name should be unique across
        // all other connectors, since it is used as a prefix for all Kafka topic names coming
        // from this connector. Only alphanumeric characters and underscores should be used.
        props.setProperty("database.server.name", "postgres_cdc_source");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.dbname", checkNotNull(database));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        // we will create different slot name for each snapshot reader during backfiil task
        // execution, the original slot name will be used by enumerator to create slot for
        // global stream split
        props.setProperty("slot.name", checkNotNull(slotName));
        // database history
        props.setProperty(
                "database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));
        // we have to enable heartbeat for PG to make sure DebeziumChangeConsumer#handleBatch
        // is invoked after job restart
        // Enable TCP keep-alive probe to verify that the database connection is still alive
        props.setProperty("database.tcpKeepAlive", String.valueOf(true));
        props.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatInterval.toMillis()));
        props.setProperty("include.schema.changes", String.valueOf(includeSchemaChanges));

        if (schemaList != null) {
            props.setProperty("schema.include.list", String.join(",", schemaList));
        }

        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        // The PostgresSource will do snapshot according to its StartupMode.
        // Do not need debezium to do the snapshot work.
        props.setProperty("snapshot.mode", "never");

        Configuration dbzConfiguration = Configuration.from(props);

        return new PostgresSourceConfig(
                subtaskId,
                startupOptions,
                Collections.singletonList(database),
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
                dbzConfiguration,
                JDBC_DRIVER,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumns,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                lsnCommitCheckpointsDelay,
                assignUnboundedChunkFirst);
    }

    /**
     * An optional list of regular expressions that match schema names to be monitored; any schema
     * name not included in the whitelist will be excluded from monitoring. By default all
     * non-system schemas will be monitored.
     */
    public void schemaList(String[] schemaList) {
        this.schemaList = Arrays.asList(schemaList);
    }

    /**
     * The name of the Postgres logical decoding plug-in installed on the server. Supported values
     * are decoderbufs and pgoutput.
     */
    public void decodingPluginName(String name) {
        this.pluginName = name;
    }

    /** The name of the PostgreSQL database from which to stream the changes. */
    public void database(String database) {
        this.database = database;
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
    public void slotName(String slotName) {
        this.slotName = slotName;
    }

    /** The interval of heartbeat events. */
    public void heartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /** The lsn commit checkpoints delay for Postgres. */
    public void setLsnCommitCheckpointsDelay(int lsnCommitCheckpointsDelay) {
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;
    }

    /**
     * The chunk key of table snapshot, captured tables are split into multiple chunks by the chunk
     * key column when read the snapshot of table.
     */
    public PostgresSourceConfigFactory chunkKeyColumn(
            ObjectPath objectPath, String chunkKeyColumn) {
        this.chunkKeyColumns.put(objectPath, chunkKeyColumn);
        return this;
    }

    public PostgresSourceConfigFactory chunkKeyColumn(Map<ObjectPath, String> chunkKeyColumns) {
        this.chunkKeyColumns.putAll(chunkKeyColumns);
        return this;
    }
}
