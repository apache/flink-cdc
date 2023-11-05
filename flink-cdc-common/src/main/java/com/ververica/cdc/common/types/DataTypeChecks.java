/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.types;

/**
 * Utilities for checking {@link DataType} and avoiding a lot of type casting and repetitive work.
 */
public final class DataTypeChecks {
    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    public static int getLength(DataType dataType) {
        return dataType.accept(LENGTH_EXTRACTOR);
    }

    private DataTypeChecks() {
        // no instantiation
    }

    // --------------------------------------------------------------------------------------------
    /** Extracts an attribute of data types that define that attribute. */
    private static class Extractor<T> extends DataTypeDefaultVisitor<T> {
        @Override
        protected T defaultMethod(DataType dataType) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid use of extractor %s. Called on data type: %s",
                            this.getClass().getSimpleName(), dataType));
        }
    }

    private static class LengthExtractor extends Extractor<Integer> {
        @Override
        public Integer visit(CharType charType) {
            return charType.getLength();
        }

        @Override
        public Integer visit(BinaryType binaryType) {
            return binaryType.getLength();
        }
    }
}
