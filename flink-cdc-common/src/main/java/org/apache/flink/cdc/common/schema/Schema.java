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

package org.apache.flink.cdc.common.schema;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Schema of a table or data collection. */
@PublicEvolving
public class Schema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Column> columns;

    private final List<String> primaryKeys;

    private final List<String> partitionKeys;

    private final Map<String, String> options;

    private final @Nullable String comment;

    // Used to index column by name
    private transient volatile Map<String, Column> nameToColumns;

    // Transiently cached fields that are lazily calculated
    private transient List<String> columnNames;

    private transient List<DataType> columnDataTypes;

    private transient DataType columnRowType;

    /**
     * Schema might be used as a LoadingCache key frequently, and maintaining a cache of hashCode
     * would be more efficient.
     */
    private transient int cachedHashCode;

    private Schema(
            List<Column> columns,
            List<String> primaryKeys,
            Map<String, String> options,
            @Nullable String comment) {
        this.columns = columns;
        this.primaryKeys = primaryKeys;
        this.options = options;
        this.comment = comment;
        this.partitionKeys = new ArrayList<>();
    }

    private Schema(
            List<Column> columns,
            List<String> primaryKeys,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable String comment) {
        this.columns = columns;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.options = options;
        this.comment = comment;
    }

    /** Returns the number of columns of this schema. */
    public int getColumnCount() {
        return columns.size();
    }

    /** Returns all {@link Column}s of this schema. */
    public List<Column> getColumns() {
        return columns;
    }

    /** Returns all column names. It does not distinguish between different kinds of columns. */
    public List<String> getColumnNames() {
        if (columnNames == null) {
            columnNames =
                    Collections.unmodifiableList(
                            columns.stream().map(Column::getName).collect(Collectors.toList()));
        }
        return columnNames;
    }

    /**
     * Returns all column data types. It does not distinguish between different kinds of columns.
     */
    public List<DataType> getColumnDataTypes() {
        if (columnDataTypes == null) {
            columnDataTypes =
                    Collections.unmodifiableList(
                            columns.stream().map(Column::getType).collect(Collectors.toList()));
        }
        return columnDataTypes;
    }

    /** Returns the primary keys of the table or data collection. */
    public List<String> primaryKeys() {
        return primaryKeys;
    }

    /** Returns the partition keys of the table or data collection. */
    public List<String> partitionKeys() {
        return partitionKeys;
    }

    /** Returns the options of the table or data collection. */
    public Map<String, String> options() {
        return options;
    }

    public String describeOptions() {
        StringBuilder stringBuilder = new StringBuilder("(");
        if (options != null && !options.isEmpty()) {
            stringBuilder.append(options);
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    /** Returns the comment of the table or data collection. */
    public String comment() {
        return comment;
    }

    /**
     * Returns the {@link Column} instance for the given column name.
     *
     * @param columnName the name of the column
     */
    public Optional<Column> getColumn(String columnName) {
        initializeNameToColumns();
        return Optional.ofNullable(nameToColumns.get(columnName));
    }

    /**
     * Converts all columns of this schema into a (possibly nested) row data type.
     *
     * @see DataTypes#ROW(DataField...)
     */
    public DataType toRowDataType() {
        if (columnRowType == null) {
            final DataField[] fields =
                    columns.stream().map(Schema::columnToField).toArray(DataField[]::new);
            // the row should never be null
            columnRowType = DataTypes.ROW(fields).notNull();
        }
        return columnRowType;
    }

    /** Returns a copy of the schema with a replaced list of {@link Column}. */
    public Schema copy(List<Column> columns) {
        return new Schema(
                columns,
                new ArrayList<>(primaryKeys),
                new ArrayList<>(partitionKeys),
                new HashMap<>(options),
                comment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Schema)) {
            return false;
        }
        Schema schema = (Schema) o;
        return Objects.equals(columns, schema.columns)
                && Objects.equals(primaryKeys, schema.primaryKeys)
                && Objects.equals(partitionKeys, schema.partitionKeys)
                && Objects.equals(options, schema.options)
                && Objects.equals(comment, schema.comment);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == 0) {
            cachedHashCode = Objects.hash(columns, primaryKeys, partitionKeys, options, comment);
        }
        return cachedHashCode;
    }

    // -----------------------------------------------------------------------------------
    private void initializeNameToColumns() {
        if (nameToColumns == null) {
            // make the method thread-safe
            synchronized (this) {
                // we need to check nullability again here
                if (nameToColumns == null) {
                    nameToColumns = new HashMap<>();
                    for (Column col : columns) {
                        nameToColumns.put(col.getName(), col);
                    }
                    nameToColumns = Collections.unmodifiableMap(nameToColumns);
                }
            }
        }
    }

    private static DataField columnToField(Column column) {
        return DataTypes.FIELD(column.getName(), column.getType());
    }

    /** Builder for configuring and creating instances of {@link Schema}. */
    public static Schema.Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("columns={");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i).asSummaryString());
            if (i != columns.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("}");
        sb.append(", primaryKeys=").append(String.join(";", primaryKeys));
        if (!partitionKeys.isEmpty()) {
            sb.append(", partitionKeys=").append(String.join(";", partitionKeys));
        }
        if (!StringUtils.isNullOrWhitespaceOnly(comment)) {
            sb.append(", comment=").append(comment);
        }
        sb.append(", options=").append(describeOptions());

        return sb.toString();
    }

    // -----------------------------------------------------------------------------------

    /** A builder for constructing an immutable but still unresolved {@link Schema}. */
    @PublicEvolving
    public static final class Builder {

        private List<Column> columns;
        private List<String> primaryKeys;
        private List<String> partitionKeys;
        private final Map<String, String> options;
        private @Nullable String comment;

        // Used to check duplicate columns
        private final Set<String> columnNames;

        public Builder() {
            this.primaryKeys = new ArrayList<>();
            this.partitionKeys = new ArrayList<>();
            this.options = new HashMap<>();
            this.columns = new ArrayList<>();
            this.columnNames = new HashSet<>();
        }

        /**
         * Declares a partition key constraint for a set of given columns. Partition key uniquely
         * identify a row in a table. Neither of columns in a partition can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder partitionKey(String... columnNames) {
            this.partitionKeys = Arrays.asList(columnNames);
            return this;
        }

        /**
         * Declares a partition key constraint for a set of given columns. Partition key uniquely
         * identify a row in a table. Neither of columns in a partition can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder partitionKey(List<String> columnNames) {
            this.partitionKeys = new ArrayList<>(columnNames);
            return this;
        }

        /** Adopts all fields of the given row as physical columns of the schema. */
        public Builder fromRowDataType(DataType type) {
            Preconditions.checkNotNull(type, "Data type must not be null.");
            Preconditions.checkArgument(type.is(DataTypeRoot.ROW), "Data type of ROW expected.");
            final List<DataType> fieldDataTypes = type.getChildren();
            final List<String> fieldNames = ((RowType) type).getFieldNames();
            IntStream.range(0, fieldDataTypes.size())
                    .forEach(i -> physicalColumn(fieldNames.get(i), fieldDataTypes.get(i)));
            return this;
        }

        /**
         * Declares a physical column that is appended to this schema.
         *
         * @param columnName column name
         * @param type data type of the column
         */
        public Builder physicalColumn(String columnName, DataType type) {
            checkColumn(columnName, type);
            columns.add(Column.physicalColumn(columnName, type));
            return this;
        }

        /**
         * Declares a physical column that is appended to this schema.
         *
         * @param columnName column name
         * @param type data type of the column
         * @param comment description of the column
         */
        public Builder physicalColumn(String columnName, DataType type, String comment) {
            checkColumn(columnName, type);
            columns.add(Column.physicalColumn(columnName, type, comment));
            return this;
        }

        /**
         * Declares a physical column that is appended to this schema.
         *
         * @param columnName column name
         * @param type data type of the column
         * @param comment description of the column
         * @param defaultValue default value of the column
         */
        public Builder physicalColumn(
                String columnName, DataType type, String comment, String defaultValue) {
            checkColumn(columnName, type);
            columns.add(Column.physicalColumn(columnName, type, comment, defaultValue));
            return this;
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * @param columnName column name
         * @param type data type of the column
         */
        public Builder metadataColumn(String columnName, DataType type) {
            checkColumn(columnName, type);
            columns.add(Column.metadataColumn(columnName, type));
            return this;
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * @param columnName column name
         * @param type data type of the column
         * @param metadataKey the key of metadata
         */
        public Builder metadataColumn(String columnName, DataType type, String metadataKey) {
            checkColumn(columnName, type);
            columns.add(Column.metadataColumn(columnName, type, metadataKey, null));
            return this;
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * @param columnName column name
         * @param type data type of the column
         * @param metadataKey the key of metadata
         * @param comment description of the column
         */
        public Builder metadataColumn(
                String columnName, DataType type, String metadataKey, String comment) {
            checkColumn(columnName, type);
            columns.add(Column.metadataColumn(columnName, type, metadataKey, comment));
            return this;
        }

        public Builder column(Column column) {
            checkColumn(column.getName(), column.getType());
            columns.add(column);
            return this;
        }

        private void checkColumn(String columnName, DataType type) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(type, "Data type must not be null.");
            if (columnNames.contains(columnName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column names must be unique, the duplicate column name: '%s'",
                                columnName));
            }
            columnNames.add(columnName);
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(String... columnNames) {
            return primaryKey(Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(List<String> columnNames) {
            this.primaryKeys = new ArrayList<>(columnNames);
            return this;
        }

        /** Declares options. */
        public Builder options(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        /** Declares an option. */
        public Builder option(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        /** Declares table comment. */
        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        /** Set new columns. */
        public Builder setColumns(List<Column> columns) {
            this.columns = columns;
            return this;
        }

        /** Returns an instance of a {@link Schema}. */
        public Schema build() {
            return new Schema(columns, primaryKeys, partitionKeys, options, comment);
        }
    }
}
