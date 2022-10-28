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

package com.ververica.cdc.connectors.db2;

import com.ververica.cdc.connectors.db2.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import io.debezium.connector.db2.Db2Connector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Source for DB2 CDC connector. */
public class Db2Source {
    private static final String DB2_DATABASE_SERVER_NAME = "db2_cdc_source";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder for Db2Source.
     *
     * @param <T> Output type of the source
     */
    public static class Builder<T> {
        private String hostname;
        private int port = 50000;
        private String username;
        private String password;
        private String database;
        // Should be in "schema.table" format
        private String[] tableList;
        private Properties dbzProperties;
        private StartupOptions startupOptions = StartupOptions.initial();
        private DebeziumDeserializationSchema<T> deserializer;

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", Db2Connector.class.getCanonicalName());
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));
            props.setProperty("database.dbname", checkNotNull(database));
            props.setProperty("database.server.name", DB2_DATABASE_SERVER_NAME); // Hard-coded here
            props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));

            if (tableList != null) {
                props.setProperty("table.whitelist", String.join(",", tableList));
            }
            if (dbzProperties != null) {
                props.putAll(dbzProperties);
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

            return new DebeziumSourceFunction<>(
                    deserializer, props, null, Validator.getDefaultValidator());
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        public Builder<T> debeziumProperties(Properties debeziumProperties) {
            this.dbzProperties = debeziumProperties;
            return this;
        }

        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        /** Specifies the startup options. */
        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }
    }
}
