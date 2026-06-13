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

package org.apache.flink.cdc.connectors.lancedb.utils;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Type mapping helpers between Flink CDC and Arrow/Lance schemas. */
public class LanceDbTypeUtils {

    public static final String CDC_OP_COLUMN = "_cdc_op";
    public static final String CDC_DELETED_COLUMN = "_cdc_deleted";
    public static final String CDC_EVENT_TIME_COLUMN = "_cdc_event_time";

    private LanceDbTypeUtils() {}

    public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(
            Schema schema, boolean includeCdcMetadata) {
        List<Field> fields = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()) {
                continue;
            }
            if (includeCdcMetadata && isCdcMetadataColumn(column.getName())) {
                throw new IllegalStateException(
                        "Source schema contains reserved LanceDB CDC metadata column "
                                + column.getName()
                                + ". Rename the source column or use sink.changelog-mode=append-only.");
            }
            fields.add(
                    toArrowField(
                            column.getName(), column.getType(), column.getType().isNullable()));
        }
        if (includeCdcMetadata) {
            fields.add(Field.nullable(CDC_OP_COLUMN, new ArrowType.Utf8()));
            fields.add(Field.nullable(CDC_DELETED_COLUMN, new ArrowType.Bool()));
            fields.add(Field.nullable(CDC_EVENT_TIME_COLUMN, new ArrowType.Int(64, true)));
        }
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    public static boolean isCdcMetadataColumn(String columnName) {
        return CDC_OP_COLUMN.equals(columnName)
                || CDC_DELETED_COLUMN.equals(columnName)
                || CDC_EVENT_TIME_COLUMN.equals(columnName);
    }

    public static Field toArrowField(String name, DataType dataType, boolean nullable) {
        ArrowType arrowType = toArrowType(dataType);
        List<Field> children = Collections.emptyList();
        if (dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            children =
                    Collections.singletonList(
                            toArrowField("element", elementType, elementType.isNullable()));
        }
        return new Field(name, new FieldType(nullable, arrowType, null, null), children);
    }

    public static ArrowType toArrowType(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new ArrowType.Utf8();
            case BOOLEAN:
                return new ArrowType.Bool();
            case BINARY:
            case VARBINARY:
                return new ArrowType.Binary();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return new ArrowType.Decimal(
                        decimalType.getPrecision(), decimalType.getScale(), 128);
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME_WITHOUT_TIME_ZONE:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            case ARRAY:
                validateSupportedArrayElement((ArrayType) dataType);
                return new ArrowType.List();
            case MAP:
            case ROW:
            case VARIANT:
                throw new UnsupportedOperationException(
                        "LanceDB connector does not support "
                                + dataType.asSummaryString()
                                + " yet. Use scalar or ARRAY columns.");
            default:
                throw new UnsupportedOperationException(
                        "Unsupported LanceDB type: " + dataType.asSummaryString());
        }
    }

    private static void validateSupportedArrayElement(ArrayType arrayType) {
        switch (arrayType.getElementType().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return;
            default:
                throw new UnsupportedOperationException(
                        "LanceDB connector only supports ARRAY elements of string, boolean, integer, float, and double types. Unsupported element type: "
                                + arrayType.getElementType().asSummaryString());
        }
    }

    public static void validateCompatible(
            String datasetPath,
            org.apache.arrow.vector.types.pojo.Schema expected,
            org.apache.arrow.vector.types.pojo.Schema actual) {
        List<Field> expectedFields = expected.getFields();
        List<Field> actualFields = actual.getFields();
        if (expectedFields.size() != actualFields.size()) {
            throw new IllegalStateException(
                    "Existing Lance dataset "
                            + datasetPath
                            + " has "
                            + actualFields.size()
                            + " fields but source schema expects "
                            + expectedFields.size()
                            + ".");
        }
        for (int i = 0; i < expectedFields.size(); i++) {
            Field expectedField = expectedFields.get(i);
            Field actualField = actualFields.get(i);
            validateCompatibleField(
                    datasetPath, expectedField, actualField, expectedField.getName());
        }
    }

    public static void validateCompatibleField(
            String datasetPath, Field expectedField, Field actualField, String fieldPath) {
        if (!expectedField.getName().equals(actualField.getName())
                || !expectedField.getType().equals(actualField.getType())
                || expectedField.isNullable() != actualField.isNullable()) {
            throw new IllegalStateException(
                    "Existing Lance dataset "
                            + datasetPath
                            + " field mismatch at "
                            + fieldPath
                            + ". Expected "
                            + expectedField
                            + " but actual is "
                            + actualField
                            + ".");
        }
        List<Field> expectedChildren = expectedField.getChildren();
        List<Field> actualChildren = actualField.getChildren();
        if (expectedChildren.size() != actualChildren.size()) {
            throw new IllegalStateException(
                    "Existing Lance dataset "
                            + datasetPath
                            + " field "
                            + fieldPath
                            + " has "
                            + actualChildren.size()
                            + " child fields but source schema expects "
                            + expectedChildren.size()
                            + ".");
        }
        for (int i = 0; i < expectedChildren.size(); i++) {
            Field expectedChild = expectedChildren.get(i);
            validateCompatibleField(
                    datasetPath,
                    expectedChild,
                    actualChildren.get(i),
                    fieldPath + "." + expectedChild.getName());
        }
    }
}
