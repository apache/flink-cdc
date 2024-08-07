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

package org.apache.flink.cdc.connectors.jdbc.catalog;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Column for Jdbc catalog. */
public class JdbcColumn implements Serializable {
    /** The name of the column. */
    private final String name;
    /** The type of the column. e.g. `VARCHAR(10)` for MySQL. */
    private final String columnType;
    /** The type of the column data. e.g. `VARCHAR` for MySQL. */
    private final String dataType;
    /** The position of the column within the table (starting at 0). */
    private final int position;
    /** The column nullability.IS_NULLABLE in information_schema.COLUMNS. */
    private final boolean isNullable;
    /** The length of the column type. e.g. `VARCHAR(10)` length is 10 */
    private final Integer length;
    /** The precision of the column type e.g. `decimal(10, 2)` precision is 10 */
    protected Integer precision;
    /** The precision of the column type. e.g. `DECIMAL(10, 2)` scale is 2 */
    @Nullable private final Integer scale;
    /** The column comment. */
    @Nullable private final String comment;
    /** The default value for the column. */
    @Nullable private final String defaultValue;

    public JdbcColumn(
            String name,
            String columnType,
            String dataType,
            int position,
            boolean isNullable,
            @Nullable Integer length,
            @Nullable Integer precision,
            @Nullable Integer scale,
            @Nullable String comment,
            @Nullable String defaultValue) {
        this.name = name;
        this.columnType = columnType;
        this.dataType = dataType;
        this.position = position;
        this.isNullable = isNullable;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
        this.comment = comment;
        this.defaultValue = defaultValue;
    }

    private JdbcColumn(Builder builder) {
        this.name = builder.name;
        this.columnType = builder.columnType;
        this.dataType = builder.dataType;
        this.position = builder.position;
        this.isNullable = builder.isNullable;
        this.length = builder.length;
        this.precision = builder.precision;
        this.scale = builder.scale;
        this.comment = builder.comment;
        this.defaultValue = builder.defaultValue;
    }

    /** Builder for Jdbc JdbcColumn. */
    public static class Builder {
        private String name;
        private String columnType;
        private String dataType;
        private int position;
        private boolean isNullable;
        private Integer length;
        private Integer precision;
        private Integer scale;
        private String comment;
        private String defaultValue;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder columnType(String columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder dataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder position(int position) {
            this.position = position;
            return this;
        }

        public Builder isNullable(boolean isNullable) {
            this.isNullable = isNullable;
            return this;
        }

        public Builder length(@Nullable Integer length) {
            this.length = length;
            return this;
        }

        public Builder precision(@Nullable Integer precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(@Nullable Integer scale) {
            this.scale = scale;
            return this;
        }

        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        public Builder defaultValue(@Nullable String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public JdbcColumn build() {
            return new JdbcColumn(this);
        }
    }

    public String getName() {
        return name;
    }

    public String getColumnType() {
        return columnType;
    }

    public String getDataType() {
        return dataType;
    }

    public int getPosition() {
        return position;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getPrecision() {
        return precision;
    }

    @Nullable
    public Integer getScale() {
        return scale;
    }

    @Nullable
    public String getComment() {
        return comment;
    }

    @Nullable
    public String getDefaultValue() {
        return defaultValue;
    }
}
