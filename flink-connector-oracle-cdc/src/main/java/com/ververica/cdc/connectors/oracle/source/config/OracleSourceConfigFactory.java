/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source.config;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfigFactory;
import com.ververica.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnector;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A factory to initialize {@link OracleSourceConfig}. */
public class OracleSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "oracle_logminer";
    private static final String DRIVER_ClASS_NAME = "oracle.jdbc.OracleDriver";

    @Nullable private String url;
    private List<String> schemaList;

    /** Url to use when connecting to the Oracle database server. */
    public JdbcSourceConfigFactory url(@Nullable String url) {
        this.url = url;
        return this;
    }

    /**
     * An optional list of regular expressions that match schema names to be monitored; any schema
     * name not included in the whitelist will be excluded from monitoring. By default all
     * non-system schemas will be monitored.
     */
    public JdbcSourceConfigFactory schemaList(String... schemaList) {
        this.schemaList = Arrays.asList(schemaList);
        return this;
    }

    /** Creates a new {@link OracleSourceConfig} for the given subtask {@code subtaskId}. */
    public OracleSourceConfig create(int subtaskId) {
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
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        // database history
        props.setProperty(
                "database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));
        props.setProperty("connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));
        // disable tombstones
        props.setProperty("tombstones.on.delete", String.valueOf(false));

        if (url != null) {
            props.setProperty("database.url", url);
        } else {
            checkNotNull(hostname, "hostname is required when url is not configured");
            props.setProperty("database.hostname", hostname);
            checkNotNull(port, "port is required when url is not configured");
            props.setProperty("database.port", String.valueOf(port));
        }

        if (schemaList != null) {
            props.setProperty("schema.whitelist", String.join(",", schemaList));
        }

        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        return new OracleSourceConfig(
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                props,
                dbzConfiguration,
                DRIVER_ClASS_NAME,
                url,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize);
    }
}
