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

package org.apache.flink.cdc.connectors.sqlserver.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnector;

import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for creating {@link SqlServerSourceConfig}. */
public class SqlServerSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "sqlserver_transaction_log_source";
    private static final String DRIVER_ClASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    @Override
    public SqlServerSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        if (startupOptions.startupMode != StartupMode.INITIAL
                && startupOptions.startupMode != StartupMode.SNAPSHOT
                && startupOptions.startupMode != StartupMode.LATEST_OFFSET
                && startupOptions.startupMode != StartupMode.TIMESTAMP) {
            throw new UnsupportedOperationException(
                    "Unsupported startup mode: " + startupOptions.startupMode);
        }
        this.startupOptions = startupOptions;
        return this;
    }

    @Override
    public SqlServerSourceConfig create(int subtask) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        props.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());

        // set database history impl to flink database history
        props.setProperty(
                "schema.history.internal", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty(
                "schema.history.internal.instance.name", UUID.randomUUID() + "_" + subtask);

        // SQL Server is Debezium's only multi-partition connector, and for those Metrics tags
        // the JMX ObjectName with "task.id" as well as the topic prefix. Kafka Connect supplies
        // that property; Flink never does, so Metrics#metricName passes null to
        // Sanitizer.jmxSanitize and every SQL Server task dies with a NullPointerException
        // before it reads a row. The subtask index is the natural analogue of a Connect task id
        // and keeps the name unique within the TaskManager JVM.
        props.setProperty("task.id", String.valueOf(subtask));

        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the SQL Server database
        // server that you want Debezium to capture. The logical name should be unique across
        // all other connectors, since it is used as a prefix for all Kafka topic names
        // emanating from this connector. Only alphanumeric characters and underscores should be
        // used.
        // Debezium 2.0 renamed "database.server.name" to "topic.prefix".
        props.setProperty("topic.prefix", DATABASE_SERVER_NAME);
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("schema.history.internal.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        // Debezium 2.0 replaced "database.dbname" with the multi-database "database.names";
        // it is a required option and validation fails when it is empty.
        props.setProperty("database.names", checkNotNull(databaseList.get(0)));
        // The mssql-jdbc driver bundled with Debezium 2.x defaults "encrypt" to true, while the
        // 9.x driver used by Debezium 1.9 defaulted it to false. Keep the historical Flink CDC
        // behavior; users can opt into TLS with "debezium.database.encrypt".
        props.setProperty("database.encrypt", "false");

        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        switch (startupOptions.startupMode) {
            case INITIAL:
                props.setProperty("snapshot.mode", "initial");
                break;
            case SNAPSHOT:
                // Debezium snapshot only; do not start the streaming phase
                props.setProperty("snapshot.mode", "initial_only");
                break;
            case LATEST_OFFSET:
                props.setProperty("snapshot.mode", "schema_only");
                break;
            case TIMESTAMP:
                // We recover stream reading from timestamp-mapped LSN and only need schema
                // snapshot.
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
                closeIdleReaders,
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
                chunkKeyColumn,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
    }
}
