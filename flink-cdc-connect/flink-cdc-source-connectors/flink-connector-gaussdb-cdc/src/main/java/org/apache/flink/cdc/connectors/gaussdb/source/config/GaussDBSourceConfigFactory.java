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

package org.apache.flink.cdc.connectors.gaussdb.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import io.debezium.config.Configuration;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for creating {@link GaussDBSourceConfig}. */
public class GaussDBSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private static final String DATABASE_SERVER_NAME = "gaussdb_cdc_source";
    private static final String CONNECTOR_CLASS_NAME =
            "io.debezium.connector.gaussdb.GaussDBConnector";
    private static final String JDBC_DRIVER_CLASS_NAME = "com.huawei.gaussdb.jdbc.Driver";

    private Duration heartbeatInterval = GaussDBSourceOptions.HEARTBEAT_INTERVAL.defaultValue();
    private List<String> schemaList =
            Collections.singletonList(GaussDBSourceOptions.SCHEMA_NAME.defaultValue());
    private String slotName;
    private String decodingPluginName = GaussDBSourceOptions.DECODING_PLUGIN_NAME.defaultValue();

    public GaussDBSourceConfigFactory() {
        this.port = GaussDBSourceOptions.PORT.defaultValue();
    }

    /**
     * Creates a {@link GaussDBSourceConfigFactory} from Flink {@link ReadableConfig}.
     *
     * <p>Required options: {@code hostname}, {@code username}, {@code password}, {@code
     * database-name}, {@code slot.name}.
     */
    public static GaussDBSourceConfigFactory fromConfiguration(ReadableConfig config) {
        String hostname = requireNonEmpty(config, GaussDBSourceOptions.HOSTNAME);
        String username = requireNonEmpty(config, GaussDBSourceOptions.USERNAME);
        String password = requireNonEmpty(config, GaussDBSourceOptions.PASSWORD);
        String databaseName = requireNonEmpty(config, GaussDBSourceOptions.DATABASE_NAME);
        String slotName = requireNonEmpty(config, GaussDBSourceOptions.SLOT_NAME);

        GaussDBSourceConfigFactory factory = new GaussDBSourceConfigFactory();
        factory.hostname(hostname);
        factory.port(config.get(GaussDBSourceOptions.PORT));
        factory.username(username);
        factory.password(password);
        factory.databaseList(databaseName);

        factory.slotName(slotName);
        factory.decodingPluginName(config.get(GaussDBSourceOptions.DECODING_PLUGIN_NAME));
        factory.heartbeatInterval(config.get(GaussDBSourceOptions.HEARTBEAT_INTERVAL));

        String schemaName = config.get(GaussDBSourceOptions.SCHEMA_NAME);
        factory.schemaList(schemaName);

        String tableName = config.get(GaussDBSourceOptions.TABLE_NAME);
        if (tableName != null && !tableName.trim().isEmpty()) {
            factory.tableList(schemaName + "." + tableName);
        }

        factory.fetchSize(config.get(GaussDBSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE));
        factory.connectionPoolSize(config.get(GaussDBSourceOptions.CONNECTION_POOL_SIZE));
        return factory;
    }

    /**
     * @deprecated Prefer {@link #fromConfiguration(ReadableConfig)} and call {@link #create(int)}
     *     with subtask id.
     */
    @Deprecated
    public static GaussDBSourceConfig create(ReadableConfig config) {
        return fromConfiguration(config).create(0);
    }

    /** The name of the GaussDB schema to monitor. */
    public void schemaList(String... schemaList) {
        if (schemaList == null || schemaList.length == 0) {
            this.schemaList =
                    Collections.singletonList(GaussDBSourceOptions.SCHEMA_NAME.defaultValue());
        } else {
            this.schemaList = Arrays.asList(schemaList);
        }
    }

    /** The name of the GaussDB logical decoding slot. */
    public void slotName(String slotName) {
        this.slotName = slotName;
    }

    /** The name of the GaussDB logical decoding plug-in installed on the server. */
    public void decodingPluginName(String decodingPluginName) {
        this.decodingPluginName = decodingPluginName;
    }

    /** The interval of heartbeat events. */
    public void heartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    @Override
    public GaussDBSourceConfig create(int subtask) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);

        Properties props = new Properties();
        props.setProperty("connector.class", CONNECTOR_CLASS_NAME);
        props.setProperty("plugin.name", checkNotNull(decodingPluginName));
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("slot.name", checkNotNull(slotName));

        props.setProperty(
                "database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));
        props.setProperty("database.tcpKeepAlive", String.valueOf(true));
        props.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatInterval.toMillis()));
        props.setProperty("include.schema.changes", String.valueOf(includeSchemaChanges));

        if (databaseList != null && !databaseList.isEmpty()) {
            props.setProperty("database.include.list", String.join(",", databaseList));
        }
        if (schemaList != null && !schemaList.isEmpty()) {
            props.setProperty("schema.include.list", String.join(",", schemaList));
        }
        if (tableList != null && !tableList.isEmpty()) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        // The GaussDBSource will do snapshot according to its StartupMode.
        // Do not need debezium to do the snapshot work.
        props.setProperty("snapshot.mode", "never");

        Configuration dbzConfiguration = Configuration.from(props);
        return new GaussDBSourceConfig(
                startupOptions,
                databaseList,
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
                JDBC_DRIVER_CLASS_NAME,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst,
                slotName,
                decodingPluginName);
    }

    private static String requireNonEmpty(ReadableConfig config, ConfigOption<String> option) {
        String value = config.get(option);
        checkNotNull(value, "Required option '%s' is missing.", option.key());
        checkArgument(
                !value.trim().isEmpty(), "Required option '%s' must not be blank.", option.key());
        return value;
    }
}
