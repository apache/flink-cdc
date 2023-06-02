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

package com.ververica.cdc.connectors.sqlserver.source.config;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfigFactory;
import com.ververica.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnector;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for creating {@link SqlServerSourceConfig}. */
public class SqlServerSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "sqlserver_transaction_log_source";
    private static final String DRIVER_ClASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private List<String> schemaList;

    /**
     * An optional list of regular expressions that match schema names to be monitored; any schema
     * name not included in the whitelist will be excluded from monitoring. By default, all
     * non-system schemas will be monitored.
     */
    public JdbcSourceConfigFactory schemaList(String... schemaList) {
        this.schemaList = Arrays.asList(schemaList);
        return this;
    }

    @Override
    public SqlServerSourceConfig create(int subtask) {
        Properties props = new Properties();
        props.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());

        // set database history impl to flink database history
        props.setProperty(
                "database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);

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
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));

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

        Configuration dbzConfiguration = Configuration.from(props);
        return new SqlServerSourceConfig(
                startupOptions,
                databaseList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                props,
                dbzConfiguration,
                DRIVER_ClASS_NAME,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn);
    }
}
