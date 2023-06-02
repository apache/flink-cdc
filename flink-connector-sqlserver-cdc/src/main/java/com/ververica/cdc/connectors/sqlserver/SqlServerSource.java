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

package com.ververica.cdc.connectors.sqlserver;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.connector.sqlserver.SqlServerConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume transaction
 * log for SqlServer.
 */
public class SqlServerSource {

    private static final String DATABASE_SERVER_NAME = "sqlserver_transaction_log_source";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link SqlServerSource}. */
    public static class Builder<T> {

        private int port = 1433; // default 1433 port
        private String hostname;
        private String database;
        private String username;
        private String password;
        private String[] tableList;
        private Properties dbzProperties;
        private StartupOptions startupOptions = StartupOptions.initial();
        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /** Integer port number of the SQL Server database server. */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        /** The name of the SQL Server database from which to stream the changes. */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /**
         * An optional comma-separated list of regular expressions that match fully-qualified table
         * identifiers for tables that you want Debezium to capture; any table that is not included
         * in table.include.list is excluded from capture. Each identifier is of the form
         * schemaName.tableName. By default, the connector captures all non-system tables for the
         * designated schemas. Must not be used with table.exclude.list.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        /** Username to use when connecting to the SQL Server database server. */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to use when connecting to the SQL Server database server. */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /** The Debezium SqlServer connector properties. For example, "snapshot.mode". */
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
            props.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());
            // hard code server name, because we don't need to distinguish it, docs:
            // Logical name that identifies and provides a namespace for the SQL Server database
            // server that you want Debezium to capture. The logical name should be unique across
            // all other connectors, since it is used as a prefix for all Kafka topic names
            // emanating from this connector. Only alphanumeric characters and underscores should be
            // used.
            props.setProperty("database.server.name", DATABASE_SERVER_NAME);
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
            props.setProperty("database.dbname", checkNotNull(database));

            if (tableList != null) {
                props.setProperty("table.include.list", String.join(",", tableList));
            }

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

            return new DebeziumSourceFunction<>(
                    deserializer, props, null, new SqlServerValidator(props));
        }
    }
}
