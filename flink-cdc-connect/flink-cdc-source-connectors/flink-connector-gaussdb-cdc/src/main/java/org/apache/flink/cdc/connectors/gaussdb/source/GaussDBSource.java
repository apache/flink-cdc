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
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.Validator;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder entrypoint for the Debezium-based GaussDB CDC source function.
 *
 * <p>Use {@link #builder()} for the non-parallel Debezium source function fallback, or {@link
 * #incrementalBuilder()} for the parallel incremental snapshot source.
 */
@Experimental
public class GaussDBSource {

    private static final String CONNECTOR_CLASS = "io.debezium.connector.gaussdb.GaussDBConnector";
    private static final String SERVER_NAME = "gaussdb_cdc_source";
    private static final String DEFAULT_PLUGIN_NAME = "mppdb_decoding";
    private static final long DEFAULT_HEARTBEAT_MS = Duration.ofMinutes(5).toMillis();

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Get a builder for the parallel, FLIP-27 GaussDB source with incremental snapshot support.
     *
     * @deprecated Prefer {@link GaussDBSourceBuilder} directly. This is kept for compatibility with
     *     existing tests and examples.
     */
    @Deprecated
    public static <T> GaussDBSourceBuilder<T> incrementalBuilder() {
        return GaussDBSourceBuilder.GaussDBIncrementalSource.builder();
    }

    /** Builder class of Debezium-based {@link DebeziumSourceFunction}. */
    public static class Builder<T> {

        private String pluginName = DEFAULT_PLUGIN_NAME;
        private String slotName = "flink";
        private int port = 8000;
        private Integer haPort;
        private String hostname;
        private String database;
        private String username;
        private String password;
        private String[] schemaList;
        private String[] tableList;
        private Properties dbzProperties;
        private DebeziumDeserializationSchema<T> deserializer;

        /** The name of the GaussDB logical decoding plug-in installed on the server. */
        public Builder<T> decodingPluginName(String name) {
            this.pluginName = name;
            return this;
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /** Integer port number of the GaussDB database server. */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        /** Optional HA port number for GaussDB logical replication. */
        public Builder<T> haPort(int haPort) {
            this.haPort = haPort;
            return this;
        }

        /** The name of the GaussDB database from which to stream the changes. */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /**
         * An optional list of regular expressions that match schema names to be monitored; any
         * schema name not included in the whitelist will be excluded from monitoring. By default
         * all non-system schemas will be monitored.
         */
        public Builder<T> schemaList(String... schemaList) {
            this.schemaList = schemaList;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers for
         * tables to be monitored; any table not included in the whitelist will be excluded from
         * monitoring. Each identifier is of the form schemaName.tableName. By default the connector
         * will monitor every non-system table in each monitored schema.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        /**
         * Name of the GaussDB database to use when connecting to the GaussDB database server.
         */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to use when connecting to the GaussDB database server. */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /**
         * The name of the GaussDB logical decoding slot that was created for streaming changes from
         * a particular plug-in for a particular database/schema.
         */
        public Builder<T> slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }

        /** The Debezium GaussDB connector properties. */
        public Builder<T> debeziumProperties(Properties properties) {
            this.dbzProperties = properties;
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

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", CONNECTOR_CLASS);
            props.setProperty("plugin.name", pluginName);
            props.setProperty("database.server.name", SERVER_NAME);
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.dbname", checkNotNull(database));
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("slot.name", checkNotNull(slotName));
            props.setProperty("database.tcpKeepAlive", String.valueOf(true));
            props.setProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_MS));

            if (haPort != null) {
                props.setProperty("ha-port", String.valueOf(haPort));
            }
            if (schemaList != null) {
                props.setProperty("schema.include.list", String.join(",", schemaList));
            }
            if (tableList != null) {
                props.setProperty("table.include.list", String.join(",", tableList));
            }
            if (dbzProperties != null) {
                props.putAll(dbzProperties);
            }

            return new DebeziumSourceFunction<>(
                    deserializer, props, null, Validator.getDefaultValidator());
        }
    }
}
