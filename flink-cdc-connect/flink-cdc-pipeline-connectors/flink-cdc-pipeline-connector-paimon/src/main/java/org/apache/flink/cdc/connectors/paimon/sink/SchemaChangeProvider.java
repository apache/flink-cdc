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
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.schema.SchemaChange;

import java.util.ArrayList;
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
        List<SchemaChange> result = new ArrayList<>();
        result.add(
                SchemaChange.addColumn(
                        columnWithPosition.getAddColumn().getName(),
                        LogicalTypeConversion.toDataType(
                                DataTypeUtils.toFlinkDataType(
                                                columnWithPosition.getAddColumn().getType())
                                        .getLogicalType()),
                        columnWithPosition.getAddColumn().getComment()));
        // if default value express exists, we need to set the default value to the table
        // option
        Column column = columnWithPosition.getAddColumn();
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
        List<SchemaChange> result = new ArrayList<>();
        result.add(
                SchemaChange.addColumn(
                        columnWithPosition.getAddColumn().getName(),
                        LogicalTypeConversion.toDataType(
                                DataTypeUtils.toFlinkDataType(
                                                columnWithPosition.getAddColumn().getType())
                                        .getLogicalType()),
                        columnWithPosition.getAddColumn().getComment(),
                        move));
        // if default value express exists, we need to set the default value to the table
        // option
        Column column = columnWithPosition.getAddColumn();
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
     * Creates a SchemaChange object to update the data type of a column.
     *
     * @param oldColumnName The name of the column whose data type is to be updated.
     * @param newType The new DataType for the column.
     * @return A SchemaChange object representing the update of the column's data type.
     */
    public static SchemaChange updateColumnType(String oldColumnName, DataType newType) {
        return SchemaChange.updateColumnType(
                oldColumnName,
                LogicalTypeConversion.toDataType(
                        DataTypeUtils.toFlinkDataType(newType).getLogicalType()));
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

            if (INVALID_OR_MISSING_DATATIME.equals(defaultValue)) {
                return DEFAULT_DATETIME;
            }
        }

        return defaultValue;
    }
}
