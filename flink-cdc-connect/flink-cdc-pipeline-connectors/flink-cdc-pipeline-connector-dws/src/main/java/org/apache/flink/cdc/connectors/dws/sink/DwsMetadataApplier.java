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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getLength;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** Applies schema evolution events to GaussDB DWS through JDBC DDL statements. */
public class DwsMetadataApplier implements MetadataApplier, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DwsMetadataApplier.class);
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_SCHEMA = "public";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final boolean caseSensitive;
    private final String defaultSchema;
    private final boolean enableDnPartition;
    private final String distributionKey;

    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public DwsMetadataApplier(
            String jdbcUrl,
            String username,
            String password,
            boolean caseSensitive,
            String defaultSchema,
            boolean enableDnPartition,
            String distributionKey) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.caseSensitive = caseSensitive;
        this.defaultSchema = normalizeDefaultSchema(defaultSchema);
        this.enableDnPartition = enableDnPartition;
        this.distributionKey = distributionKey;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return EnumSet.of(
                CREATE_TABLE,
                ADD_COLUMN,
                ALTER_COLUMN_TYPE,
                DROP_COLUMN,
                RENAME_COLUMN,
                TRUNCATE_TABLE,
                DROP_TABLE);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws SchemaEvolveException {
        ensureConnectionConfigured(event);

        SchemaChangeEventVisitor.<Void, SchemaEvolveException>visit(
                event,
                addColumnEvent -> {
                    applyAddColumnEvent(addColumnEvent);
                    return null;
                },
                alterColumnTypeEvent -> {
                    applyAlterColumnTypeEvent(alterColumnTypeEvent);
                    return null;
                },
                createTableEvent -> {
                    applyCreateTableEvent(createTableEvent);
                    return null;
                },
                dropColumnEvent -> {
                    applyDropColumnEvent(dropColumnEvent);
                    return null;
                },
                dropTableEvent -> {
                    applyDropTableEvent(dropTableEvent);
                    return null;
                },
                renameColumnEvent -> {
                    applyRenameColumnEvent(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    applyTruncateTableEvent(truncateTableEvent);
                    return null;
                });
    }

    String buildCreateTableSql(CreateTableEvent event) {
        TableId tableId = event.tableId();
        Schema schema = event.getSchema();
        List<String> columnDefinitions =
                schema.getColumns().stream()
                        .map(this::buildColumnDefinition)
                        .collect(Collectors.toList());

        if (!schema.primaryKeys().isEmpty()) {
            columnDefinitions.add(
                    schema.primaryKeys().stream()
                            .map(this::quoteIdentifier)
                            .collect(Collectors.joining(", ", "PRIMARY KEY (", ")")));
        }

        StringBuilder sql =
                new StringBuilder(
                        String.format(
                                "CREATE TABLE IF NOT EXISTS %s (%s)",
                                formatTableIdentifier(tableId),
                                String.join(", ", columnDefinitions)));
        String distributionClause = buildDistributionClause();
        if (distributionClause != null) {
            sql.append(' ').append(distributionClause);
        }
        return sql.toString();
    }

    String buildAddColumnSql(TableId tableId, Column column) {
        return String.format(
                "ALTER TABLE %s ADD COLUMN %s",
                formatTableIdentifier(tableId), buildColumnDefinition(column));
    }

    String buildDropColumnSql(TableId tableId, String columnName) {
        return String.format(
                "ALTER TABLE %s DROP COLUMN IF EXISTS %s",
                formatTableIdentifier(tableId), quoteIdentifier(columnName));
    }

    String buildTruncateTableSql(TableId tableId) {
        return String.format("TRUNCATE TABLE %s", formatTableIdentifier(tableId));
    }

    String buildDropTableSql(TableId tableId) {
        return String.format("DROP TABLE IF EXISTS %s", formatTableIdentifier(tableId));
    }

    String buildRenameColumnSql(TableId tableId, String oldName, String newName) {
        return String.format(
                "ALTER TABLE %s RENAME COLUMN %s TO %s",
                formatTableIdentifier(tableId), quoteIdentifier(oldName), quoteIdentifier(newName));
    }

    String buildAlterColumnTypeSql(TableId tableId, String columnName, DataType newType) {
        return String.format(
                "ALTER TABLE %s ALTER COLUMN %s TYPE %s",
                formatTableIdentifier(tableId), quoteIdentifier(columnName), toDwsType(newType));
    }

    private void applyCreateTableEvent(CreateTableEvent event) {
        executeDdl(event, buildCreateTableSql(event));
    }

    private void applyAddColumnEvent(AddColumnEvent event) {
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            executeDdl(
                    event, buildAddColumnSql(event.tableId(), columnWithPosition.getAddColumn()));
        }
    }

    private void applyDropColumnEvent(DropColumnEvent event) {
        for (String columnName : event.getDroppedColumnNames()) {
            executeDdl(event, buildDropColumnSql(event.tableId(), columnName));
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event) {
        for (Map.Entry<String, String> entry : event.getNameMapping().entrySet()) {
            executeDdl(
                    event, buildRenameColumnSql(event.tableId(), entry.getKey(), entry.getValue()));
        }
    }

    private void applyAlterColumnTypeEvent(AlterColumnTypeEvent event) {
        for (Map.Entry<String, DataType> entry : event.getTypeMapping().entrySet()) {
            executeDdl(
                    event,
                    buildAlterColumnTypeSql(event.tableId(), entry.getKey(), entry.getValue()));
        }
    }

    private void applyTruncateTableEvent(TruncateTableEvent event) {
        executeDdl(event, buildTruncateTableSql(event.tableId()));
    }

    private void applyDropTableEvent(DropTableEvent event) {
        executeDdl(event, buildDropTableSql(event.tableId()));
    }

    private void executeDdl(SchemaChangeEvent event, String sql) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
            LOG.info("Applied schema change for {} with SQL: {}", event.tableId(), sql);
        } catch (SQLException e) {
            throw new SchemaEvolveException(event, "Failed to apply DDL: " + sql, e);
        }
    }

    private String buildColumnDefinition(Column column) {
        StringBuilder builder =
                new StringBuilder()
                        .append(quoteIdentifier(column.getName()))
                        .append(' ')
                        .append(toDwsType(column.getType()));

        if (column.getDefaultValueExpression() != null) {
            builder.append(" DEFAULT ").append(formatDefaultValue(column));
        }
        if (!column.getType().isNullable()) {
            builder.append(" NOT NULL");
        }
        return builder.toString();
    }

    private String formatDefaultValue(Column column) {
        String expression = column.getDefaultValueExpression();
        switch (column.getType().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return '\'' + expression.replace("'", "''") + '\'';
            default:
                return expression;
        }
    }

    private String formatTableIdentifier(TableId tableId) {
        String schemaName = tableId.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = defaultSchema;
        }
        return quoteIdentifier(schemaName) + "." + quoteIdentifier(tableId.getTableName());
    }

    private String buildDistributionClause() {
        if (!enableDnPartition || distributionKey == null || distributionKey.trim().isEmpty()) {
            return null;
        }

        String quotedDistributionKeys =
                java.util.Arrays.stream(distributionKey.split(","))
                        .map(String::trim)
                        .filter(key -> !key.isEmpty())
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        if (quotedDistributionKeys.isEmpty()) {
            return null;
        }
        return "DISTRIBUTE BY HASH (" + quotedDistributionKeys + ")";
    }

    private String quoteIdentifier(String identifier) {
        String normalized = normalizeIdentifier(identifier);
        if (!caseSensitive) {
            return normalized;
        }
        return '"' + normalized.replace("\"", "\"\"") + '"';
    }

    private String normalizeIdentifier(String identifier) {
        String value = identifier.trim();
        return caseSensitive ? value : value.toLowerCase(Locale.ROOT);
    }

    private String toDwsType(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return String.format("DECIMAL(%d, %d)", getPrecision(type), getScale(type));
            case CHAR:
                return String.format("CHAR(%d)", getLength(type));
            case VARCHAR:
                int length = getLength(type);
                return length >= 10_485_760 ? "TEXT" : String.format("VARCHAR(%d)", length);
            case BINARY:
            case VARBINARY:
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return String.format("TIME(%d)", normalizePrecision(getPrecision(type)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return String.format("TIMESTAMP(%d)", normalizePrecision(getPrecision(type)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return String.format("TIMESTAMPTZ(%d)", normalizePrecision(getPrecision(type)));
            case ARRAY:
                return "TEXT";
            case MAP:
            case ROW:
                return "JSON";
            default:
                throw new IllegalArgumentException(
                        "Unsupported data type for GaussDB DWS DDL: " + type);
        }
    }

    private static int normalizePrecision(int precision) {
        return Math.max(0, Math.min(6, precision));
    }

    private void ensureConnectionConfigured(SchemaChangeEvent event) {
        if (jdbcUrl == null || username == null || password == null) {
            throw new SchemaEvolveException(
                    event,
                    "GaussDB DWS metadata applier requires jdbc-url, username, and password.",
                    null);
        }
    }

    private static String normalizeDefaultSchema(String defaultSchema) {
        if (defaultSchema == null || defaultSchema.trim().isEmpty()) {
            return DEFAULT_SCHEMA;
        }
        return defaultSchema.trim();
    }
}
