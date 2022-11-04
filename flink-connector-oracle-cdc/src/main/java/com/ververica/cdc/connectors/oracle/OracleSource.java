/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.oracle;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import io.debezium.connector.oracle.OracleConnector;

import javax.annotation.Nullable;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume log miner.
 */
public class OracleSource {

    private static final String DATABASE_SERVER_NAME = "oracle_logminer";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link OracleSource}. */
    public static class Builder<T> {

        private Integer port = 1521; // default 1521 port
        private String hostname;
        private String database;
        private String username;
        private String password;
        private String url;
        private String[] tableList;
        private String[] schemaList;
        private Properties dbzProperties;
        private StartupOptions startupOptions = StartupOptions.initial();
        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> hostname(@Nullable String hostname) {
            this.hostname = hostname;
            return this;
        }

        /** Integer port number of the Oracle database server. */
        public Builder<T> port(@Nullable Integer port) {
            this.port = port;
            return this;
        }

        /** Url to use when connecting to the Oracle database server. */
        public Builder<T> url(@Nullable String url) {
            this.url = url;
            return this;
        }

        /**
         * An optional list of regular expressions that match database names to be monitored; any
         * database name not included in the whitelist will be excluded from monitoring. By default
         * all databases will be monitored.
         */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers for
         * tables to be monitored; any table not included in the list will be excluded from
         * monitoring. Each identifier is of the form schemaName.tableName. By default the connector
         * will monitor every non-system table in each monitored database.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
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

        /** Name of the Oracle database to use when connecting to the Oracle database server. */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to use when connecting to the Oracle database server. */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /** The Debezium Oracle connector properties. For example, "snapshot.mode". */
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

        /** Specifies the startup options. */
        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", OracleConnector.class.getCanonicalName());
            // Logical name that identifies and provides a namespace for the particular Oracle
            // database server being
            // monitored. The logical name should be unique across all other connectors, since it is
            // used as a prefix
            // for all Kafka topic names emanating from this connector. Only alphanumeric characters
            // and
            // underscores should be used.
            props.setProperty("database.server.name", DATABASE_SERVER_NAME);
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));

            if (url != null) {
                props.setProperty("database.url", url);
            }
            if (hostname != null) {
                props.setProperty("database.hostname", hostname);
            }
            if (port != null) {
                props.setProperty("database.port", String.valueOf(port));
            }
            props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
            props.setProperty("database.dbname", checkNotNull(database));
            if (schemaList != null) {
                props.setProperty("schema.whitelist", String.join(",", schemaList));
            }
            if (tableList != null) {
                props.setProperty("table.include.list", String.join(",", tableList));
            }

            DebeziumOffset specificOffset = null;
            switch (startupOptions.startupMode) {
                case INITIAL:
                    props.setProperty("snapshot.mode", "initial");
                    break;

                case LATEST_OFFSET:
                    props.setProperty("snapshot.mode", "schema_only");
                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            if (dbzProperties != null) {
                props.putAll(dbzProperties);
            }

            if (url == null) {
                checkNotNull(hostname, "hostname is required when url is not configured");
                props.setProperty("database.hostname", hostname);
                checkNotNull(port, "port is required when url is not configured");
                props.setProperty("database.port", String.valueOf(port));
            }

            return new DebeziumSourceFunction<>(
                    deserializer, props, specificOffset, new OracleValidator(props));
        }
    }
}
