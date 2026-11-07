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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.schema.Column;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.fluss.utils.FlussConversions.toFlussType;

/** Validates Fluss table schemas against pipeline columns. */
class FlussSchemaValidator {

    static void validateAddedColumns(AddColumnEvent event, Schema target, TablePath tablePath) {
        List<DataField> fields = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (AddColumnEvent.ColumnWithPosition addition : event.getAddedColumns()) {
            Column column = addition.getAddColumn();
            if (!names.add(column.getName())) {
                throw new IllegalArgumentException(
                        "Duplicate added column " + column.getName() + " for table " + tablePath);
            }
            fields.add(new DataField(column.getName(), toFlussType(column.getType())));
        }
        validateColumns(new RowType(fields), target.getRowType(), tablePath);
    }

    static void validateSchema(Schema expected, Schema target, TablePath tablePath) {
        RowType expectedRow = expected.getRowType();
        RowType targetRow = target.getRowType();
        validateColumns(expectedRow, targetRow, tablePath);
        for (DataField targetField : targetRow.getFields()) {
            if (expectedRow.getFieldIndex(targetField.getName()) < 0
                    && !targetField.getType().isNullable()) {
                throw new IllegalArgumentException(
                        "Target-only column "
                                + targetField.getName()
                                + " in table "
                                + tablePath
                                + " is NOT NULL but the sink writes NULL for unmapped columns");
            }
        }
    }

    private static void validateColumns(RowType expected, RowType target, TablePath tablePath) {
        for (DataField expectedField : expected.getFields()) {
            String name = expectedField.getName();
            int index = target.getFieldIndex(name);
            if (index < 0) {
                throw new IllegalArgumentException(
                        "Column " + name + " is missing from Fluss table " + tablePath);
            }
            DataType expectedType = expectedField.getType();
            DataType targetType = target.getTypeAt(index);
            if (!expectedType.copy(false).equals(targetType.copy(false))
                    || (expectedType.isNullable() && !targetType.isNullable())) {
                throw new IllegalArgumentException(
                        "Column "
                                + name
                                + " in Fluss table "
                                + tablePath
                                + " has type "
                                + targetType
                                + ", which cannot contain pipeline type "
                                + expectedType);
            }
        }
    }
}
