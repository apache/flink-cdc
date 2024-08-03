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

package org.apache.flink.cdc.connectors.db2.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Schema;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2Partition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils.createDb2Connection;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class Db2SchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(Db2SchemaUtils.class);

    private static volatile Db2Dialect db2Dialect;

    public static List<String> listSchemas(Db2SourceConfig sourceConfig, String namespace) {
        try (Db2Connection jdbc = createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
            return listSchemas(jdbc, namespace);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list schemas: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(Db2SourceConfig sourceConfig, @Nullable String dbName) {
        try (Db2Connection jdbc = createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
            List<TableId> tableIds = new ArrayList<>();
            List<TableId> tableIdList =
                    Db2ConnectionUtils.listTables(
                                    jdbc, sourceConfig.getDbzConnectorConfig().getTableFilters())
                            .stream()
                            .map(Db2SchemaUtils::toCdcTableId)
                            .collect(Collectors.toList());
            tableIds.addAll(tableIdList);
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(
            Db2SourceConfig sourceConfig, Db2Partition partition, TableId tableId) {
        try (Db2Connection jdbc = createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
            return getTableSchema(partition, tableId, sourceConfig, jdbc);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> listSchemas(JdbcConnection jdbc, String namespace)
            throws SQLException {
        LOG.info("Read list of available schemas");
        final List<String> schemaNames = new ArrayList<>();

        jdbc.query(
                "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA",
                rs -> {
                    while (rs.next()) {
                        schemaNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available schemas are: {}", schemaNames);
        return schemaNames;
    }

    public static Schema getTableSchema(
            Db2Partition partition,
            TableId tableId,
            Db2SourceConfig sourceConfig,
            Db2Connection jdbc) {
        // fetch table schemas
        Db2Schema db2Schema = new Db2Schema();
        TableChanges.TableChange tableSchema =
                db2Schema.getTableSchema(
                        jdbc,
                        toDbzTableId(tableId),
                        sourceConfig
                                .getDbzConnectorConfig()
                                .getTableFilters()
                                .dataCollectionFilter());
        return toSchema(tableSchema.getTable());
    }

    public static Schema getTableSchema(
            io.debezium.relational.TableId tableId,
            Db2SourceConfig sourceConfig,
            Db2Connection jdbc) {
        // fetch table schemas
        Db2Schema db2Schema = new Db2Schema();
        TableChanges.TableChange tableSchema =
                db2Schema.getTableSchema(
                        jdbc,
                        tableId,
                        sourceConfig
                                .getDbzConnectorConfig()
                                .getTableFilters()
                                .dataCollectionFilter());
        return toSchema(tableSchema.getTable());
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream().map(Db2SchemaUtils::toColumn).collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(), Db2TypeUtils.fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    public static org.apache.flink.cdc.common.event.TableId toCdcTableId(
            io.debezium.relational.TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.schema(), dbzTableId.table());
    }

    private Db2SchemaUtils() {}
}
