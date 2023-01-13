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

package com.ververica.cdc.connectors.mysql;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import io.debezium.connector.mysql.MySqlConnector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ververica.cdc.debezium.DebeziumSourceFunction.LEGACY_IMPLEMENTATION_KEY;
import static com.ververica.cdc.debezium.DebeziumSourceFunction.LEGACY_IMPLEMENTATION_VALUE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume binlog.
 *
 * @deprecated please use {@link com.ververica.cdc.connectors.mysql.source.MySqlSource} instead
 *     which supports more rich features, e.g. parallel reading from historical data. The {@link
 *     MySqlSource} will be dropped in the future version.
 */
@Deprecated
public class MySqlSource {

    private static final String DATABASE_SERVER_NAME = "mysql_binlog_source";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder class of {@link MySqlSource}.
     *
     * @deprecated please use {@link
     *     com.ververica.cdc.connectors.mysql.source.MySqlSource#builder()} instead which supports
     *     more rich features, e.g. parallel reading from historical data. The {@link
     *     MySqlSource.Builder} will be dropped in the future version.
     */
    @Deprecated
    public static class Builder<T> {

        private int port = 3306; // default 3306 port
        private String hostname;
        private String[] databaseList;
        private String username;
        private String password;
        private Integer serverId;
        private String serverTimeZone;
        private String[] tableList;
        private Properties dbzProperties;
        private StartupOptions startupOptions = StartupOptions.initial();
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
         * A numeric ID of this database client, which must be unique across all currently-running
         * database processes in the MySQL cluster. This connector joins the MySQL database cluster
         * as another server (with this unique ID) so it can read the binlog. By default, a random
         * number is generated between 5400 and 6400, though we recommend setting an explicit value.
         */
        public Builder<T> serverId(int serverId) {
            this.serverId = serverId;
            return this;
        }

        /** The Debezium MySQL connector properties. For example, "snapshot.mode". */
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
            props.setProperty("connector.class", MySqlConnector.class.getCanonicalName());
            // hard code server name, because we don't need to distinguish it, docs:
            // Logical name that identifies and provides a namespace for the particular MySQL
            // database
            // server/cluster being monitored. The logical name should be unique across all other
            // connectors,
            // since it is used as a prefix for all Kafka topic names emanating from this connector.
            // Only alphanumeric characters and underscores should be used.
            props.setProperty("database.server.name", DATABASE_SERVER_NAME);
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
            // debezium use "long" mode to handle unsigned bigint by default,
            // but it'll cause lose of precise when the value is larger than 2^63,
            // so use "precise" mode to avoid it.
            props.put("bigint.unsigned.handling.mode", "precise");

            if (serverId != null) {
                props.setProperty("database.server.id", String.valueOf(serverId));
            }
            if (databaseList != null) {
                props.setProperty("database.whitelist", String.join(",", databaseList));
            }
            if (tableList != null) {
                props.setProperty("table.whitelist", String.join(",", tableList));
            }
            if (serverTimeZone != null) {
                props.setProperty("database.serverTimezone", serverTimeZone);
            }

            DebeziumOffset specificOffset = null;
            switch (startupOptions.startupMode) {
                case INITIAL:
                    props.setProperty("snapshot.mode", "initial");
                    break;

                case EARLIEST_OFFSET:
                    props.setProperty("snapshot.mode", "never");
                    break;

                case LATEST_OFFSET:
                    props.setProperty("snapshot.mode", "schema_only");
                    break;

                case SPECIFIC_OFFSETS:
                    // if binlog offset is specified, 'snapshot.mode=schema_only_recovery' must
                    // be configured. It only snapshots the schemas, not the data,
                    // and continue binlog reading from the specified offset
                    props.setProperty("snapshot.mode", "schema_only_recovery");

                    specificOffset = new DebeziumOffset();
                    Map<String, String> sourcePartition = new HashMap<>();
                    sourcePartition.put("server", DATABASE_SERVER_NAME);
                    specificOffset.setSourcePartition(sourcePartition);

                    Map<String, Object> sourceOffset = new HashMap<>();
                    sourceOffset.put("file", startupOptions.binlogOffset.getFilename());
                    sourceOffset.put("pos", startupOptions.binlogOffset.getPosition());
                    specificOffset.setSourceOffset(sourceOffset);
                    break;

                case TIMESTAMP:
                    checkNotNull(deserializer);
                    props.setProperty("snapshot.mode", "never");
                    deserializer =
                            new SeekBinlogToTimestampFilter<>(
                                    startupOptions.binlogOffset.getTimestampSec() * 1000,
                                    deserializer);
                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            if (dbzProperties != null) {
                props.putAll(dbzProperties);
                // Add default configurations for compatibility when set the legacy mysql connector
                // implementation
                if (LEGACY_IMPLEMENTATION_VALUE.equals(
                        dbzProperties.get(LEGACY_IMPLEMENTATION_KEY))) {
                    props.put("transforms", "snapshotasinsert");
                    props.put(
                            "transforms.snapshotasinsert.type",
                            "io.debezium.connector.mysql.transforms.ReadToInsertEvent");
                }
            }

            return new DebeziumSourceFunction<>(
                    deserializer, props, specificOffset, new MySqlValidator(props));
        }
    }
}
