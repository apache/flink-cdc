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

package org.apache.flink.cdc.connectors.vitess;

import org.apache.flink.cdc.connectors.vitess.config.SchemaAdjustmentMode;
import org.apache.flink.cdc.connectors.vitess.config.TabletType;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;

import io.debezium.connector.vitess.VitessConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read and process vitess database changes. The
 * Vitess connector subscribes to VTGate's VStream gRPC service. VTGate is a lightweight, stateless
 * gRPC server, which is part of the Vitess cluster setup.
 */
public class VitessSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link VitessSource}. */
    public static class Builder<T> {

        private String pluginName = "decoderbufs";
        private String name = "flink";
        private int port = 15991; // default 15991 port
        private String hostname;
        private String keyspace;
        private String username;
        private String password;
        private TabletType tabletType = TabletType.RDONLY;
        private String shard;
        private String gtid = "current";
        private Boolean stopOnReshard = false;
        private Boolean tombstonesOnDelete = true;
        private String[] messageKeyColumns;
        private SchemaAdjustmentMode schemaNameAdjustmentMode = SchemaAdjustmentMode.NONE;
        private String[] tableIncludeList;
        private String[] tableExcludeList;
        private String[] columnIncludeList;
        private String[] columnExcludeList;
        private Properties dbzProperties;
        private DebeziumDeserializationSchema<T> deserializer;

        /**
         * The name of the Vitess logical decoding plug-in installed on the server. Supported values
         * are decoderbufs
         */
        public Builder<T> decodingPluginName(String name) {
            this.pluginName = name;
            return this;
        }

        /** Hostname of the VTGate’s VStream server. */
        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /** Integer port number of the VTGate’s VStream server. */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        /**
         * The name of the keyspace (a.k.a database). If no shard is specified, it reads change
         * events from all shards in the keyspace.
         */
        public Builder<T> keyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        /**
         * An optional name of the shard from which to stream the changes. If not configured, in
         * case of unsharded keyspace, the connector streams changes from the only shard, in case of
         * sharded keyspace, the connector streams changes from all shards in the keyspace. We
         * recommend not configuring it in order to stream from all shards in the keyspace because
         * it has better support for reshard operation. If configured, for example, -80, the
         * connector will stream changes from the -80 shard.
         */
        public Builder<T> shard(String shard) {
            this.shard = shard;
            return this;
        }

        /**
         * An optional GTID position for a shard to stream from. This has to be set together with
         * vitess.shard. If not configured, the connector streams changes from the latest position
         * for the given shard.
         */
        public Builder<T> gtid(String gtid) {
            this.gtid = gtid;
            return this;
        }

        /**
         * Controls Vitess flag stop_on_reshard. true - the stream will be stopped after a reshard
         * operation. false - the stream will be automatically migrated for the new shards after a
         * reshard operation. If set to true, you should also consider setting vitess.gtid in the
         * configuration.
         */
        public Builder<T> stopOnReshard(Boolean stopOnReshard) {
            this.stopOnReshard = stopOnReshard;
            return this;
        }

        /**
         * * Controls whether a delete event is followed by a tombstone event. true - a delete
         * operation is represented by a delete event and a subsequent tombstone event. false - only
         * a delete event is emitted.
         */
        public Builder<T> tombstonesOnDelete(Boolean tombstonesOnDelete) {
            this.tombstonesOnDelete = tombstonesOnDelete;
            return this;
        }

        /**
         * A semicolon separated list of tables with regular expressions that match table column
         * names. The connector maps values in matching columns to key fields in change event
         * records that it sends to Kafka topics. This is useful when a table does not have a
         * primary key, or when you want to order change event records in a Kafka topic according to
         * a field that is not a primary key. Separate entries with semicolons. Insert a colon
         * between the fully-qualified table name and its regular expression. The format is:
         * keyspace-name.table-name:_regexp_;…​
         */
        public Builder<T> messageKeyColumns(String[] messageKeyColumns) {
            this.messageKeyColumns = messageKeyColumns;
            return this;
        }

        /**
         * Specifies how schema names should be adjusted for compatibility with the message
         * converter used by the connector. Possible settings: avro replaces the characters that
         * cannot be used in the Avro type name with underscore. none does not apply any adjustment.
         */
        public Builder<T> schemaNameAdjustmentMode(SchemaAdjustmentMode schemaNameAdjustmentMode) {
            this.schemaNameAdjustmentMode = schemaNameAdjustmentMode;
            return this;
        }

        /**
         * The type of Tablet (hence MySQL) from which to stream the changes: MASTER represents
         * streaming from the master MySQL instance REPLICA represents streaming from the replica
         * slave MySQL instance RDONLY represents streaming from the read-only slave MySQL instance.
         */
        public Builder<T> tabletType(TabletType tabletType) {
            this.tabletType = tabletType;
            return this;
        }

        /** The username of the Vitess database server (VTGate gRPC). */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** The password of the Vitess database server (VTGate gRPC). */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Unique name for the connector. Attempting to register again with the same name will fail.
         * This property is required by all Kafka Connect connectors. Default is "flink".
         */
        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        /**
         * An optional, comma-separated list of regular expressions that match fully-qualified table
         * identifiers for tables whose changes you want to capture. Any table not included in
         * table.include.list does not have its changes captured. Each identifier is of the form
         * keyspace.tableName. By default, the connector captures changes in every non-system table
         * in each schema whose changes are being captured. Do not also set the table.exclude.list
         * property.
         */
        public Builder<T> tableIncludeList(String... tableIncludeList) {
            this.tableIncludeList = tableIncludeList;
            return this;
        }

        /**
         * An optional, comma-separated list of regular expressions that match fully-qualified table
         * identifiers for tables whose changes you do not want to capture. Any table not included
         * in table.exclude.list has it changes captured. Each identifier is of the form
         * keyspace.tableName. Do not also set the table.include.list property.
         */
        public Builder<T> tableExcludeList(String... tableExcludeList) {
            this.tableExcludeList = tableExcludeList;
            return this;
        }

        /**
         * An optional, comma-separated list of regular expressions that match the fully-qualified
         * names of columns that should be included in change event record values. Fully-qualified
         * names for columns are of the form keyspace.tableName.columnName. Do not also set the
         * column.exclude.list property.
         */
        public Builder<T> columnIncludeList(String... columnIncludeList) {
            this.columnIncludeList = columnIncludeList;
            return this;
        }

        /**
         * An optional, comma-separated list of regular expressions that match the fully-qualified
         * names of columns that should be excluded from change event record values. Fully-qualified
         * names for columns are of the form keyspace.tableName.columnName. Do not also set the
         * column.include.list property.
         */
        public Builder<T> columnExcludeList(String... columnExcludeList) {
            this.columnExcludeList = columnExcludeList;
            return this;
        }

        /** The Debezium Vitess connector properties. */
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
            props.setProperty("connector.class", VitessConnector.class.getCanonicalName());
            props.setProperty("plugin.name", pluginName);
            props.setProperty("name", name);
            // hard code server name, because we don't need to distinguish it, docs:
            // Logical name that identifies and provides a namespace for the particular Vitess
            // Vtgate server/cluster being monitored. The logical name should be unique across
            // all other connectors, since it is used as a prefix for all Kafka topic names coming
            // from this connector. Only alphanumeric characters and underscores should be used.
            props.setProperty("database.server.name", "vitess_cdc_source");
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("vitess.keyspace", checkNotNull(keyspace));
            props.setProperty("vitess.tablet.type", tabletType.name());

            if (username != null) {
                props.setProperty("vitess.database.user", username);
            }
            if (password != null) {
                props.setProperty("vitess.database.password", password);
            }

            if (shard != null) {
                props.setProperty("vitess.shard", shard);
            }
            props.setProperty("vitess.gtid", checkNotNull(gtid));

            if (messageKeyColumns != null) {
                props.setProperty("message.key.columns", String.join(",", messageKeyColumns));
            }

            props.setProperty(
                    "schema.name.adjustment.mode", schemaNameAdjustmentMode.name().toLowerCase());
            props.setProperty("vitess.stop_on_reshard", stopOnReshard.toString());
            props.setProperty("tombstones.on.delete", tombstonesOnDelete.toString());

            // The maximum number of tasks that should be created for this connector.
            // The Vitess connector always uses a single task and therefore does not use this value,
            // so the default is always acceptable.
            props.setProperty("tasks.max", "1");

            if (tableIncludeList != null) {
                props.setProperty("table.include.list", String.join(",", tableIncludeList));
            }
            if (tableExcludeList != null) {
                props.setProperty("table.exclude.list", String.join(",", tableExcludeList));
            }
            if (columnIncludeList != null) {
                props.setProperty("column.include.list", String.join(",", columnIncludeList));
            }
            if (columnExcludeList != null) {
                props.setProperty("column.exclude.list", String.join(",", columnExcludeList));
            }
            if (dbzProperties != null) {
                dbzProperties.forEach(props::put);
            }

            return new DebeziumSourceFunction<>(
                    deserializer, props, null, new VitessValidator(props));
        }
    }
}
