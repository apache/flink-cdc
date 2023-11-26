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

package com.ververica.cdc.connectors.starrocks.sink;

import javax.annotation.Nullable;

import java.util.Optional;

import static com.ververica.cdc.common.utils.Preconditions.checkNotNull;

/** Describe a column of StarRocks table. These metas are from information_schema.COLUMNS. */
public class StarRocksColumn {

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

    private StarRocksColumn(
            String columnName,
            int ordinalPosition,
            String dataType,
            boolean isNullable,
            @Nullable Integer columnSize,
            @Nullable Integer decimalDigits,
            @Nullable String columnComment) {
        this.columnName = checkNotNull(columnName);
        this.ordinalPosition = ordinalPosition;
        this.dataType = checkNotNull(dataType);
        this.isNullable = isNullable;
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
        return "StarRocksColumn{"
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
                + ", columnSize="
                + columnSize
                + ", decimalDigits="
                + decimalDigits
                + ", columnComment='"
                + columnComment
                + '\''
                + '}';
    }

    /** Build a {@link StarRocksColumn}. */
    public static class Builder {

        private String columnName;
        private int ordinalPosition;
        private String dataType;
        private boolean isNullable;
        private Integer columnSize;
        private Integer decimalDigits;
        private String columnComment;

        public StarRocksColumn.Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public StarRocksColumn.Builder setOrdinalPosition(int ordinalPosition) {
            this.ordinalPosition = ordinalPosition;
            return this;
        }

        public StarRocksColumn.Builder setDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public StarRocksColumn.Builder setNullable(boolean isNullable) {
            this.isNullable = isNullable;
            return this;
        }

        public StarRocksColumn.Builder setColumnSize(Integer columnSize) {
            this.columnSize = columnSize;
            return this;
        }

        public StarRocksColumn.Builder setDecimalDigits(Integer decimalDigits) {
            this.decimalDigits = decimalDigits;
            return this;
        }

        public StarRocksColumn.Builder setColumnComment(String columnComment) {
            this.columnComment = columnComment;
            return this;
        }

        public StarRocksColumn build() {
            return new StarRocksColumn(
                    columnName,
                    ordinalPosition,
                    dataType,
                    isNullable,
                    columnSize,
                    decimalDigits,
                    columnComment);
        }
    }
}
