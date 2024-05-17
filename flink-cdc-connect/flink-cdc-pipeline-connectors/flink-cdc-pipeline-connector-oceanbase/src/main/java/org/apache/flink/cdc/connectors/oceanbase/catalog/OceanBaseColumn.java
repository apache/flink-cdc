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

package org.apache.flink.cdc.connectors.oceanbase.catalog;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Describe a column of OceanBase table. These metas are from information_schema.COLUMNS. */
public class OceanBaseColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The name of the column. COLUMN_NAME in information_schema.COLUMNS. */
    private final String columnName;

    /**
     * The position of the column within the table (starting at 0). ORDINAL_POSITION in
     * information_schema.COLUMNS.
     */
    private final int ordinalPosition;

    /** The column data type. DATA_TYPE in information_schema.COLUMNS. */
    private final String dataType;

    /** The column nullability.IS_NULLABLE in information_schema.COLUMNS. */
    private final boolean isNullable;

    /** The default value for the column. COLUMN_DEFAULT in information_schema.COLUMNS. */
    @Nullable private final String defaultValue;

    /**
     * The column size. COLUMN_SIZE in information_schema.COLUMNS. For numeric data, this is the
     * maximum precision. For character data, this is the length in characters. For other data
     * types, this is null.
     */
    @Nullable private final Integer columnSize;

    /**
     * The number of fractional digits for numeric data. This is null for other data types.
     * DECIMAL_DIGITS in information_schema.COLUMNS.
     */
    @Nullable private final Integer decimalDigits;

    /** The column comment. COLUMN_COMMENT in information_schema.COLUMNS. */
    @Nullable private final String columnComment;

    private OceanBaseColumn(
            String columnName,
            int ordinalPosition,
            String dataType,
            boolean isNullable,
            @Nullable String defaultValue,
            @Nullable Integer columnSize,
            @Nullable Integer decimalDigits,
            @Nullable String columnComment) {
        this.columnName = checkNotNull(columnName);
        this.ordinalPosition = ordinalPosition;
        this.dataType = checkNotNull(dataType);
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.columnComment = columnComment;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public Optional<String> getDefaultValue() {
        return Optional.ofNullable(defaultValue);
    }

    public Optional<Integer> getColumnSize() {
        return Optional.ofNullable(columnSize);
    }

    public Optional<Integer> getDecimalDigits() {
        return Optional.ofNullable(decimalDigits);
    }

    public Optional<String> getColumnComment() {
        return Optional.ofNullable(columnComment);
    }

    @Override
    public String toString() {
        return "OceanBaseColumn{"
                + "columnName='"
                + columnName
                + '\''
                + ", ordinalPosition="
                + ordinalPosition
                + ", dataType='"
                + dataType
                + '\''
                + ", isNullable="
                + isNullable
                + ", defaultValue='"
                + defaultValue
                + '\''
                + ", columnSize="
                + columnSize
                + ", decimalDigits="
                + decimalDigits
                + ", columnComment='"
                + columnComment
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OceanBaseColumn column = (OceanBaseColumn) o;
        return ordinalPosition == column.ordinalPosition
                && isNullable == column.isNullable
                && Objects.equals(columnName, column.columnName)
                && dataType.equalsIgnoreCase(column.dataType)
                && Objects.equals(defaultValue, column.defaultValue)
                && Objects.equals(columnSize, column.columnSize)
                && Objects.equals(decimalDigits, column.decimalDigits)
                && Objects.equals(columnComment, column.columnComment);
    }

    /** Build a {@link OceanBaseColumn}. */
    public static class Builder {

        private String columnName;
        private int ordinalPosition;
        private String dataType;
        private boolean isNullable = true;
        private String defaultValue;
        private Integer columnSize;
        private Integer decimalDigits;
        private String columnComment;

        public OceanBaseColumn.Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public OceanBaseColumn.Builder setOrdinalPosition(int ordinalPosition) {
            this.ordinalPosition = ordinalPosition;
            return this;
        }

        public OceanBaseColumn.Builder setDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public OceanBaseColumn.Builder setNullable(boolean isNullable) {
            this.isNullable = isNullable;
            return this;
        }

        public OceanBaseColumn.Builder setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public OceanBaseColumn.Builder setColumnSize(Integer columnSize) {
            this.columnSize = columnSize;
            return this;
        }

        public OceanBaseColumn.Builder setDecimalDigits(Integer decimalDigits) {
            this.decimalDigits = decimalDigits;
            return this;
        }

        public OceanBaseColumn.Builder setColumnComment(String columnComment) {
            this.columnComment = columnComment;
            return this;
        }

        public OceanBaseColumn build() {
            return new OceanBaseColumn(
                    columnName,
                    ordinalPosition,
                    dataType,
                    isNullable,
                    defaultValue,
                    columnSize,
                    decimalDigits,
                    columnComment);
        }
    }
}
