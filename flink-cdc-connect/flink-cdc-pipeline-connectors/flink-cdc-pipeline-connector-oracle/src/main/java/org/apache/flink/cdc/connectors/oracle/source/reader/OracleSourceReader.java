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

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.OracleValidator;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;

import io.debezium.connector.oracle.OracleConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder to build a SourceFunction which can read snapshot and continue to consume log miner.
 */
public class OracleSourceReader<T> {

    private static final String DATABASE_SERVER_NAME = "oracle_logminer";
    private DebeziumDeserializationSchema<T> deserializer;
    private OracleSourceConfig sourceConfig;
    private Integer port = 1521; // default 1521 port
    private String hostname;
    private String[] database;
    private String username;
    private String password;
    private String url;
    private String[] tableList;
    private String[] schemaList;
    private Properties dbzProperties;
    private StartupOptions startupOptions = StartupOptions.initial();

    public OracleSourceReader(
            DebeziumDeserializationSchema<T> deserializer, OracleSourceConfig sourceConfig) {
        this.deserializer = deserializer;
        this.sourceConfig = sourceConfig;
    }

    public DebeziumSourceFunction<T> build() {
        schemaList = sourceConfig.getSchemaList().toArray(new String[0]);
        port = sourceConfig.getPort();
        hostname = sourceConfig.getHostname();
        username = sourceConfig.getUsername();
        password = sourceConfig.getPassword();
        startupOptions = sourceConfig.getStartupOptions();
        url = sourceConfig.getUrl();
        database = sourceConfig.getDatabaseList().toArray(new String[0]);
        tableList = sourceConfig.getTableList().toArray(new String[0]);
        dbzProperties = sourceConfig.getDbzProperties();
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
        props.setProperty("database.dbname", String.join(",", database));
        if (schemaList != null) {
            props.setProperty("schema.include.list", String.join(",", schemaList));
        }
        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }
        // we need this in order not to lose any transaction during snapshot to streaming switch
        props.setProperty("internal.log.mining.transaction.snapshot.boundary.mode", "all");

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

        return new OracleDebeziumSourceFunction<T>(
                deserializer, props, null, new OracleValidator(props), tableList, sourceConfig);
    }
}
