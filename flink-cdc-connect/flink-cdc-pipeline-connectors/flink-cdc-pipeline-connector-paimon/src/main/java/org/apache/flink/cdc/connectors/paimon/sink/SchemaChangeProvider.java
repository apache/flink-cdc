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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.connectors.paimon.sink.utils.TypeUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The SchemaChangeProvider class provides static methods to create SchemaChange objects that
 * represent different types of schema modifications.
 */
public class SchemaChangeProvider {

    public static final String DEFAULT_DATETIME = "1970-01-01 00:00:00";
    public static final String INVALID_OR_MISSING_DATATIME = "0000-00-00 00:00:00";

    /**
     * Creates a SchemaChange object for adding a column without specifying its position.
     *
     * @param columnWithPosition The ColumnWithPosition object containing the column details and its
     *     intended position within the schema.
     * @return A SchemaChange object representing the addition of a column.
     */
    public static List<SchemaChange> add(AddColumnEvent.ColumnWithPosition columnWithPosition) {
        return add(columnWithPosition, Collections.emptyMap());
    }

    /**
     * Creates a SchemaChange object for adding a column without specifying its position. Supports
     * converting VARBINARY/BINARY/VARCHAR/STRING to BLOB type based on tableOptions configuration.
     *
     * @param columnWithPosition The ColumnWithPosition object containing the column details.
     * @param tableOptions The table options containing blob-field configuration.
     * @return A SchemaChange object representing the addition of a column.
     */
    public static List<SchemaChange> add(
            AddColumnEvent.ColumnWithPosition columnWithPosition,
            Map<String, String> tableOptions) {
        List<SchemaChange> result = new ArrayList<>();
        Column column = columnWithPosition.getAddColumn();

        org.apache.paimon.types.DataType dataType = convertToBlobIfNeeded(column, tableOptions);

        result.add(SchemaChange.addColumn(column.getName(), dataType, column.getComment()));

        // if default value express exists, we need to set the default value to the table option
        Optional.ofNullable(
                        convertInvalidTimestampDefaultValue(
                                column.getDefaultValueExpression(), column.getType()))
                .ifPresent(
                        value -> {
                            result.add(
                                    SchemaChange.updateColumnDefaultValue(
                                            new String[] {column.getName()}, value));
                        });
        return result;
    }

    /**
     * Creates a SchemaChange object for adding a column with a specified position.
     *
     * @param columnWithPosition The ColumnWithPosition object containing the column details and its
     *     intended position within the schema.
     * @param move The move operation to indicate the column's new position.
     * @return A SchemaChange object representing the addition of a column with position
     *     information.
     */
    public static List<SchemaChange> add(
            AddColumnEvent.ColumnWithPosition columnWithPosition, SchemaChange.Move move) {
        return add(columnWithPosition, move, Collections.emptyMap());
    }

    /**
     * Creates a SchemaChange object for adding a column with a specified position. Supports
     * converting VARBINARY/BINARY/VARCHAR/STRING to BLOB type based on tableOptions configuration.
     *
     * @param columnWithPosition The ColumnWithPosition object containing the column details.
     * @param move The move operation to indicate the column's new position.
     * @param tableOptions The table options containing blob-field configuration.
     * @return A SchemaChange object representing the addition of a column with position
     *     information.
     */
    public static List<SchemaChange> add(
            AddColumnEvent.ColumnWithPosition columnWithPosition,
            SchemaChange.Move move,
            Map<String, String> tableOptions) {
        List<SchemaChange> result = new ArrayList<>();
        Column column = columnWithPosition.getAddColumn();

        org.apache.paimon.types.DataType dataType = convertToBlobIfNeeded(column, tableOptions);

        result.add(
                SchemaChange.addColumn(
                        columnWithPosition.getAddColumn().getName(),
                        dataType,
                        columnWithPosition.getAddColumn().getComment(),
                        move));
        // if default value express exists, we need to set the default value to the table
        // option
        Optional.ofNullable(
                        convertInvalidTimestampDefaultValue(
                                column.getDefaultValueExpression(), column.getType()))
                .ifPresent(
                        value -> {
                            result.add(
                                    SchemaChange.updateColumnDefaultValue(
                                            new String[] {column.getName()}, value));
                        });
        return result;
    }

    /**
     * Convert CDC VARBINARY/BINARY/VARCHAR/STRING type to Paimon BLOB type if configured.
     *
     * @param column The CDC column definition.
     * @param tableOptions The table options containing blob-field configuration.
     * @return The Paimon DataType (BLOB if configured, otherwise original converted type).
     */
    public static org.apache.paimon.types.DataType convertToBlobIfNeeded(
            Column column, Map<String, String> tableOptions) {
        // Check if this field should be converted to BLOB type using Paimon's CoreOptions
        List<String> blobFields = CoreOptions.blobField(tableOptions);
        if (!blobFields.isEmpty() && isSupportedTypeForBlob(column.getType())) {
            if (blobFields.contains(column.getName())) {
                // Convert VARBINARY/BINARY/VARCHAR/STRING to BLOB type
                // BLOB type is always nullable in Paimon
                return DataTypes.BLOB();
            }
        }

        // Use TypeUtils.toPaimonDataType which handles VARIANT type properly
        return TypeUtils.toPaimonDataType(column.getType());
    }

    /** Check if DataType can be converted to BLOB (BINARY, VARBINARY, or VARCHAR). */
    private static boolean isSupportedTypeForBlob(DataType dataType) {
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        return typeRoot == DataTypeRoot.BINARY
                || typeRoot == DataTypeRoot.VARBINARY
                || typeRoot == DataTypeRoot.VARCHAR;
    }

    /**
     * Creates a SchemaChange object to update the data type of a column.
     *
     * @param oldColumnName The name of the column whose data type is to be updated.
     * @param newType The new DataType for the column.
     * @return A SchemaChange object representing the update of the column's data type.
     */
    public static Optional<SchemaChange> updateColumnType(
            TableSchema tableSchema, String oldColumnName, DataType newType) {
        return updateColumnType(tableSchema, oldColumnName, newType, Collections.emptyMap());
    }

    /**
     * Creates a SchemaChange object to update the data type of a column. Supports converting
     * VARBINARY/BINARY/VARCHAR/STRING to BLOB type based on tableOptions configuration.
     *
     * @param tableSchema The TableSchema object containing the current schema of the table.
     * @param oldColumnName The name of the column whose data type is to be updated.
     * @param newType The new DataType for the column.
     * @param tableOptions The table options containing blob-field configuration.
     * @return An Optional of the SchemaChange that represents the update of the column's data type.
     */
    public static Optional<SchemaChange> updateColumnType(
            TableSchema tableSchema,
            String oldColumnName,
            DataType newType,
            Map<String, String> tableOptions) {
        org.apache.paimon.types.DataType oldDataType =
                tableSchema.logicalRowType().getField(oldColumnName).type();

        org.apache.paimon.types.DataType newDataType;
        // Handle BLOB type conversion using Paimon's CoreOptions
        if (isSupportedTypeForBlob(newType)) {
            List<String> blobFields = CoreOptions.blobField(tableOptions);
            if (blobFields.contains(oldColumnName)) {
                // BLOB type is always nullable in Paimon
                newDataType = DataTypes.BLOB();
            } else {
                // Use TypeUtils.toPaimonDataType which handles VARIANT type properly
                newDataType = TypeUtils.toPaimonDataType(newType);
            }
        } else {
            // Use TypeUtils.toPaimonDataType which handles VARIANT type properly
            newDataType = TypeUtils.toPaimonDataType(newType);
        }

        if (oldDataType.equalsIgnoreNullable(newDataType)) {
            // Updating a column's nullability from notnull to nullable is not allowed.
            return !oldDataType.isNullable() && newDataType.isNullable()
                    ? Optional.of(SchemaChange.updateColumnNullability(oldColumnName, true))
                    : Optional.empty();
        } else {
            if (tableSchema.primaryKeys().contains(oldColumnName)
                    || tableSchema.partitionKeys().contains(oldColumnName)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Altering column type of a primary key/partition key column for %s from %s to %s is not supported.\n"
                                        + "If you need to change the primary key/partition key type, it is recommended to recreate the table and reimport the data.",
                                oldColumnName, oldDataType, newType));
            }
            return Optional.of(
                    SchemaChange.updateColumnType(
                            oldColumnName,
                            newDataType.copy(oldDataType.isNullable() || newType.isNullable())));
        }
    }

    /**
     * Creates a SchemaChange object for renaming a column.
     *
     * @param oldColumnName The current name of the column to be renamed.
     * @param newColumnName The new name for the column.
     * @return A SchemaChange object representing the renaming of a column.
     */
    public static List<SchemaChange> rename(
            String oldColumnName, String newColumnName, Map<String, String> options) {
        List<SchemaChange> result = new ArrayList<>();
        result.add(SchemaChange.renameColumn(oldColumnName, newColumnName));
        return result;
    }

    /**
     * Creates a SchemaChange object for dropping a column.
     *
     * @param columnName The name of the column to be dropped.
     * @return A SchemaChange object representing the deletion of a column.
     */
    public static List<SchemaChange> drop(String columnName) {
        List<SchemaChange> result = new ArrayList<>();
        result.add(SchemaChange.dropColumn(columnName));
        return result;
    }

    /**
     * Creates a SchemaChange object for setting an option.
     *
     * @param key The key of the option to be set.
     * @param value The value of the option to be set.
     * @return A SchemaChange object representing the setting of an option.
     */
    public static SchemaChange setOption(String key, String value) {
        return SchemaChange.setOption(key, value);
    }

    private static String convertInvalidTimestampDefaultValue(
            String defaultValue, DataType dataType) {
        if (defaultValue == null) {
            return null;
        }

        if (dataType instanceof LocalZonedTimestampType
                || dataType instanceof TimestampType
                || dataType instanceof ZonedTimestampType) {

            if (defaultValue.startsWith(INVALID_OR_MISSING_DATATIME)) {
                return DEFAULT_DATETIME;
            }
        }

        return defaultValue;
    }
}
