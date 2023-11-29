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

package com.ververica.cdc.common.schema;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static com.ververica.cdc.common.utils.EncodingUtils.escapeIdentifier;
import static com.ververica.cdc.common.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Representation of a column in a {@link Schema}.
 *
 * <p>A table column describes either a {@link PhysicalColumn} or {@link MetadataColumn}.
 */
@PublicEvolving
public abstract class Column implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final String FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";

    protected static final String FIELD_FORMAT_NO_DESCRIPTION = "%s %s";

    protected final String name;

    protected final DataType type;

    protected final @Nullable String comment;

    protected Column(String name, DataType type, @Nullable String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
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

    /** Returns a string that summarizes this column for printing to a console. */
    public String asSummaryString() {
        if (comment == null) {
            return String.format(
                    FIELD_FORMAT_NO_DESCRIPTION, escapeIdentifier(name), type.asSummaryString());
        } else {
            return String.format(
                    FIELD_FORMAT_WITH_DESCRIPTION,
                    escapeIdentifier(name),
                    type.asSummaryString(),
                    escapeSingleQuotes(comment));
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
                && Objects.equals(comment, column.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment);
    }

    @Override
    public String toString() {
        return asSummaryString();
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
