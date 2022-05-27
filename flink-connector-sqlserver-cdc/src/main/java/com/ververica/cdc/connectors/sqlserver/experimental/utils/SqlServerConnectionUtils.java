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

package com.ververica.cdc.connectors.sqlserver.experimental.utils;

import com.ververica.cdc.connectors.sqlserver.experimental.offset.TransactionLogOffset;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerTopicSelector;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

import java.sql.SQLException;

/** Sql Server connection Utilities. */
public class SqlServerConnectionUtils {

    /** Creates a new {@link SqlServerConnection}, but not open the connection. */
    public static SqlServerConnection createSqlServerConnection(Configuration dbzConfiguration) {
        SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(dbzConfiguration);
        return new SqlServerConnection(
                sqlServerConfig.jdbcConfig(),
                Clock.system(),
                sqlServerConfig.getSourceTimestampMode(),
                null);
    }

    /**
     * Creates a new {@link SqlServerDatabaseSchema} to monitor the latest SqlServer database
     * schemas.
     */
    public static SqlServerDatabaseSchema createSqlServerDatabaseSchema(
            SqlServerConnectorConfig dbzSqlServerConfig, boolean isTableIdCaseSensitive) {
        TopicSelector<TableId> topicSelector =
                SqlServerTopicSelector.defaultSelector(dbzSqlServerConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        SqlServerValueConverters valueConverters = getValueConverters(dbzSqlServerConfig);
        return new SqlServerDatabaseSchema(
                dbzSqlServerConfig, valueConverters, topicSelector, schemaNameAdjuster);
    }

    /** Fetch current largest log sequence number in SqlServer Server. */
    public static TransactionLogOffset currentTransactionLogOffset(JdbcConnection jdbc)
            throws SQLException {
        Lsn maxTransactionLsn = ((SqlServerConnection) jdbc).getMaxTransactionLsn();
        return null;
    }

    // --------------------------------------------------------------------------------------------

    private static SqlServerValueConverters getValueConverters(
            SqlServerConnectorConfig dbzSqlServerConfig) {
        return new SqlServerValueConverters(
                dbzSqlServerConfig.getDecimalMode(),
                dbzSqlServerConfig.getTemporalPrecisionMode(),
                dbzSqlServerConfig.binaryHandlingMode());
    }

    public static boolean isTableIdCaseSensitive(JdbcConnection connection) {
        return false;
    }
}
