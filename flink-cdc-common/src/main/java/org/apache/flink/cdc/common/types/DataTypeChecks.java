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

package org.apache.flink.cdc.common.types;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

/**
 * Utilities for checking {@link DataType} and avoiding a lot of type casting and repetitive work.
 */
public final class DataTypeChecks {

    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    private static final PrecisionExtractor PRECISION_EXTRACTOR = new PrecisionExtractor();

    private static final ScaleExtractor SCALE_EXTRACTOR = new ScaleExtractor();

    private static final FieldCountExtractor FIELD_COUNT_EXTRACTOR = new FieldCountExtractor();

    private static final FieldNamesExtractor FIELD_NAMES_EXTRACTOR = new FieldNamesExtractor();

    private static final FieldTypesExtractor FIELD_TYPES_EXTRACTOR = new FieldTypesExtractor();

    private static final NestedTypesExtractor NESTED_TYPES_EXTRACTOR = new NestedTypesExtractor();

    /**
     * Checks if the given type is a composite type.
     *
     * <p>Use {@link #getFieldCount(DataType)}, {@link #getFieldNames(DataType)}, {@link
     * #getFieldTypes(DataType)} for unified handling of composite types.
     *
     * @return True if the type is composite type.
     */
    public static boolean isCompositeType(DataType dataType) {
        return dataType.getTypeRoot() == DataTypeRoot.ROW;
    }

    public static int getLength(DataType dataType) {
        return dataType.accept(LENGTH_EXTRACTOR);
    }

    public static boolean hasLength(DataType dataType, int length) {
        return getLength(dataType) == length;
    }

    /** Returns the precision of all types that define a precision implicitly or explicitly. */
    public static int getPrecision(DataType dataType) {
        return dataType.accept(PRECISION_EXTRACTOR);
    }

    /** Checks the precision of a type that defines a precision implicitly or explicitly. */
    public static boolean hasPrecision(DataType dataType, int precision) {
        return getPrecision(dataType) == precision;
    }

    /** Returns the scale of all types that define a scale implicitly or explicitly. */
    public static int getScale(DataType dataType) {
        return dataType.accept(SCALE_EXTRACTOR);
    }

    /** Checks the scale of all types that define a scale implicitly or explicitly. */
    public static boolean hasScale(DataType dataType, int scale) {
        return getScale(dataType) == scale;
    }

    /** Returns the field count of row and structured types. Other types return 1. */
    public static int getFieldCount(DataType dataType) {
        return dataType.accept(FIELD_COUNT_EXTRACTOR);
    }

    /** Returns the field names of row and structured types. */
    public static List<String> getFieldNames(DataType dataType) {
        return dataType.accept(FIELD_NAMES_EXTRACTOR);
    }

    /** Returns the field types of row and structured types. */
    public static List<DataType> getFieldTypes(DataType dataType) {
        return dataType.accept(FIELD_TYPES_EXTRACTOR);
    }

    public static List<DataType> getNestedTypes(DataType dataType) {
        return dataType.accept(NESTED_TYPES_EXTRACTOR);
    }

    /**
     * Checks whether the given {@link DataType} has a well-defined string representation when
     * calling {@link Object#toString()} on the internal data structure. The string representation
     * would be similar in SQL or in a programming language.
     *
     * <p>Note: This method might not be necessary anymore, once we have implemented a utility that
     * can convert any internal data structure to a well-defined string representation.
     */
    public static boolean hasWellDefinedString(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    private DataTypeChecks() {
        // no instantiation
    }

    // --------------------------------------------------------------------------------------------

    /** Extracts an attribute of logical types that define that attribute. */
    private static class Extractor<T> extends DataTypeDefaultVisitor<T> {
        @Override
        protected T defaultMethod(DataType dataType) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on logical type: %s",
                            this.getClass().getName(), dataType));
        }
    }

    private static class LengthExtractor extends Extractor<Integer> {
        @Override
        protected Integer defaultMethod(DataType dataType) {
            OptionalInt length = DataTypes.getLength(dataType);
            if (length.isPresent()) {
                return length.getAsInt();
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on logical type: %s",
                            this.getClass().getName(), dataType));
        }
    }

    private static class PrecisionExtractor extends Extractor<Integer> {

        @Override
        protected Integer defaultMethod(DataType dataType) {
            OptionalInt precision = DataTypes.getPrecision(dataType);
            if (precision.isPresent()) {
                return precision.getAsInt();
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on logical type: %s",
                            this.getClass().getName(), dataType));
        }
    }

    private static class ScaleExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(DecimalType decimalType) {
            return decimalType.getScale();
        }

        @Override
        public Integer visit(TinyIntType tinyIntType) {
            return 0;
        }

        @Override
        public Integer visit(SmallIntType smallIntType) {
            return 0;
        }

        @Override
        public Integer visit(IntType intType) {
            return 0;
        }

        @Override
        public Integer visit(BigIntType bigIntType) {
            return 0;
        }
    }

    private static class FieldCountExtractor extends Extractor<Integer> {

        @Override
        public Integer visit(RowType rowType) {
            return rowType.getFieldCount();
        }

        @Override
        protected Integer defaultMethod(DataType dataType) {
            return 1;
        }
    }

    private static class FieldNamesExtractor extends Extractor<List<String>> {

        @Override
        public List<String> visit(RowType rowType) {
            return rowType.getFieldNames();
        }
    }

    private static class FieldTypesExtractor extends Extractor<List<DataType>> {

        @Override
        public List<DataType> visit(RowType rowType) {
            return rowType.getFieldTypes();
        }
    }

    private static class NestedTypesExtractor extends Extractor<List<DataType>> {

        @Override
        public List<DataType> visit(ArrayType arrayType) {
            return Collections.singletonList(arrayType.getElementType());
        }

        @Override
        public List<DataType> visit(MapType mapType) {
            return Arrays.asList(mapType.getKeyType(), mapType.getValueType());
        }

        @Override
        public List<DataType> visit(RowType rowType) {
            return rowType.getFieldTypes();
        }
    }
}
