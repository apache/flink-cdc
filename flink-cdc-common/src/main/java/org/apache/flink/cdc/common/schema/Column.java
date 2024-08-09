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
import org.apache.flink.cdc.common.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.cdc.common.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.cdc.common.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Representation of a column in a {@link Schema}.
 *
 * <p>A table column describes either a {@link PhysicalColumn} or {@link MetadataColumn}.
 */
@PublicEvolving
public abstract class Column implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final String FIELD_FORMAT_WITH_DESCRIPTION_NO_DEFAULT_VALUE_EXPRESSION =
            "%s %s '%s'";

    protected static final String FIELD_FORMAT_NO_DESCRIPTION_WITH_DEFAULT_VALUE_EXPRESSION =
            "%s %s '%s'";

    protected static final String FIELD_FORMAT_WITH_DESCRIPTION_WITH_DEFAULT_VALUE_EXPRESSION =
            "%s %s '%s' '%s'";

    protected static final String FIELD_FORMAT_NO_DESCRIPTION_NO_DEFAULT_VALUE_EXPRESSION = "%s %s";

    protected final String name;

    protected final DataType type;

    protected final @Nullable String comment;

    /**
     * Save the literal value of the column's default value, For uncertain functions such as UUID(),
     * the value is null, For the current time function such as CURRENT_TIMESTAMP(), the value is
     * Unix Epoch time(1970-01-01 00:00:00).
     */
    protected final @Nullable String defaultValueExpression;

    protected Column(String name, DataType type, @Nullable String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.defaultValueExpression = null;
    }

    protected Column(
            String name,
            DataType type,
            @Nullable String comment,
            @Nullable String defaultValueExpression) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.defaultValueExpression = defaultValueExpression;
    }

    /** Returns the name of this column. */
    public String getName() {
        return name;
    }

    /** Returns the data type of this column. */
    public DataType getType() {
        return type;
    }

    @Nullable
    public String getComment() {
        return comment;
    }

    @Nullable
    public String getDefaultValueExpression() {
        return defaultValueExpression;
    }

    /** Returns a string that summarizes this column for printing to a console. */
    public String asSummaryString() {
        if (comment == null) {
            if (defaultValueExpression == null) {
                return String.format(
                        FIELD_FORMAT_NO_DESCRIPTION_NO_DEFAULT_VALUE_EXPRESSION,
                        escapeIdentifier(name),
                        type.asSummaryString());
            } else {
                return String.format(
                        FIELD_FORMAT_NO_DESCRIPTION_WITH_DEFAULT_VALUE_EXPRESSION,
                        escapeIdentifier(name),
                        type.asSummaryString(),
                        defaultValueExpression);
            }
        } else {
            if (defaultValueExpression == null) {
                return String.format(
                        FIELD_FORMAT_WITH_DESCRIPTION_NO_DEFAULT_VALUE_EXPRESSION,
                        escapeIdentifier(name),
                        type.asSummaryString(),
                        escapeSingleQuotes(comment));
            } else {
                return String.format(
                        FIELD_FORMAT_WITH_DESCRIPTION_WITH_DEFAULT_VALUE_EXPRESSION,
                        escapeIdentifier(name),
                        type.asSummaryString(),
                        escapeSingleQuotes(comment),
                        defaultValueExpression);
            }
        }
    }

    /** Returns whether the given column is a physical column of a table or metadata column. */
    public abstract boolean isPhysical();

    /** Returns a copy of the column with a replaced {@link DataType}. */
    public abstract Column copy(DataType newType);

    /** Returns a copy of the column with a replaced name. */
    public abstract Column copy(String newName);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Column)) {
            return false;
        }
        Column column = (Column) o;
        return name.equals(column.name)
                && type.equals(column.type)
                && Objects.equals(comment, column.comment)
                && Objects.equals(defaultValueExpression, column.defaultValueExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment, defaultValueExpression);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }

    /** Creates a physical column. */
    public static PhysicalColumn physicalColumn(
            String name,
            DataType type,
            @Nullable String comment,
            @Nullable String defaultValueExpression) {
        return new PhysicalColumn(name, type, comment, defaultValueExpression);
    }

    /** Creates a physical column. */
    public static PhysicalColumn physicalColumn(
            String name, DataType type, @Nullable String comment) {
        return new PhysicalColumn(name, type, comment);
    }

    /** Creates a physical column. */
    public static PhysicalColumn physicalColumn(String name, DataType type) {
        return new PhysicalColumn(name, type, null);
    }

    /** Creates a metadata column. */
    public static MetadataColumn metadataColumn(
            String name, DataType type, @Nullable String metadataKey, @Nullable String comment) {
        return new MetadataColumn(name, type, metadataKey, comment);
    }

    /** Creates a metadata column. */
    public static MetadataColumn metadataColumn(
            String name, DataType type, @Nullable String metadataKey) {
        return new MetadataColumn(name, type, metadataKey, null);
    }

    /** Creates a metadata column. */
    public static MetadataColumn metadataColumn(String name, DataType type) {
        return new MetadataColumn(name, type, null, null);
    }
}
