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

package com.alibaba.ververica.cdc.connectors.mysql.schema;

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.quote;

/** A component used to get schema by table path. */
public class MySqlSchema {
    private final MySqlConnectorConfig connectorConfig;
    private final MySqlDatabaseSchema databaseSchema;
    private final MySqlConnection jdbc;
    private final Map<TableId, TableChange> schemasByTableId;

    public MySqlSchema(Configuration dbzConf, MySqlConnection jdbc) {
        this.connectorConfig = new MySqlConnectorConfig(dbzConf);
        TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        MySqlValueConverters valueConverters = getValueConverters(connectorConfig);
        this.databaseSchema =
                new MySqlDatabaseSchema(
                        connectorConfig,
                        valueConverters,
                        topicSelector,
                        schemaNameAdjuster,
                        jdbc.isTableIdCaseSensitive());
        this.jdbc = jdbc;
        this.schemasByTableId = new HashMap<>();
    }

    /**
     * Gets table schema for the given table path. It will request to MySQL server by running `SHOW
     * CREATE TABLE` if cache missed.
     */
    public TableChange getTableSchema(TableId tableId) {
        // read schema from cache first
        TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = readTableSchema(tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private TableChange readTableSchema(TableId tableId) {
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        final String sql = "SHOW CREATE TABLE " + quote(tableId);
        try {
            jdbc.query(
                    sql,
                    rs -> {
                        if (rs.next()) {
                            final String ddl = rs.getString(2);
                            final MySqlOffsetContext offsetContext =
                                    MySqlOffsetContext.initial(connectorConfig);
                            List<SchemaChangeEvent> schemaChangeEvents =
                                    databaseSchema.parseSnapshotDdl(
                                            ddl, tableId.catalog(), offsetContext, Instant.now());
                            for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
                                for (TableChange tableChange :
                                        schemaChangeEvent.getTableChanges()) {
                                    tableChangeMap.put(tableId, tableChange);
                                }
                            }
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s by running %s", tableId, sql),
                    e);
        }
        if (!tableChangeMap.containsKey(tableId)) {
            throw new FlinkRuntimeException(
                    String.format("Can't obtain schema for table %s by running %s", tableId, sql));
        }

        return tableChangeMap.get(tableId);
    }

    private static MySqlValueConverters getValueConverters(MySqlConnectorConfig connectorConfig) {
        TemporalPrecisionMode timePrecisionMode = connectorConfig.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = connectorConfig.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
                connectorConfig
                        .getConfig()
                        .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                        bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
                bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        final boolean timeAdjusterEnabled =
                connectorConfig.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(
                decimalMode,
                timePrecisionMode,
                bigIntUnsignedMode,
                connectorConfig.binaryHandlingMode(),
                timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
    }
}
