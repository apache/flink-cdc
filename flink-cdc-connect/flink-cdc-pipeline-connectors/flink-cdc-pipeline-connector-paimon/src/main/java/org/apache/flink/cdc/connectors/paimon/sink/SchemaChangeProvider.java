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
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.schema.SchemaChange;

/**
 * The SchemaChangeProvider class provides static methods to create SchemaChange objects that
 * represent different types of schema modifications.
 */
public class SchemaChangeProvider {

    /**
     * Creates a SchemaChange object for adding a column without specifying its position.
     *
     * @param columnWithPosition The ColumnWithPosition object containing the column details and its
     *     intended position within the schema.
     * @return A SchemaChange object representing the addition of a column.
     */
    public static SchemaChange add(AddColumnEvent.ColumnWithPosition columnWithPosition) {
        return SchemaChange.addColumn(
                columnWithPosition.getAddColumn().getName(),
                LogicalTypeConversion.toDataType(
                        DataTypeUtils.toFlinkDataType(columnWithPosition.getAddColumn().getType())
                                .getLogicalType()),
                columnWithPosition.getAddColumn().getComment());
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
    public static SchemaChange add(
            AddColumnEvent.ColumnWithPosition columnWithPosition, SchemaChange.Move move) {
        return SchemaChange.addColumn(
                columnWithPosition.getAddColumn().getName(),
                LogicalTypeConversion.toDataType(
                        DataTypeUtils.toFlinkDataType(columnWithPosition.getAddColumn().getType())
                                .getLogicalType()),
                columnWithPosition.getAddColumn().getComment(),
                move);
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
    public static SchemaChange rename(String oldColumnName, String newColumnName) {
        return SchemaChange.renameColumn(oldColumnName, newColumnName);
    }

    /**
     * Creates a SchemaChange object for dropping a column.
     *
     * @param columnName The name of the column to be dropped.
     * @return A SchemaChange object representing the deletion of a column.
     */
    public static SchemaChange drop(String columnName) {
        return SchemaChange.dropColumn(columnName);
    }
}
