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

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.cdc.common.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.cdc.common.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Defines the field of a row type.
 *
 * @see RowType
 */
@PublicEvolving
public class DataField implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";

    public static final String FIELD_FORMAT_NO_DESCRIPTION = "%s %s";

    private final String name;

    private final DataType type;

    private final @Nullable String description;

    public DataField(String name, DataType type, @Nullable String description) {
        this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
        this.type = Preconditions.checkNotNull(type, "Field type must not be null.");
        this.description = description;
    }

    public DataField(String name, DataType type) {
        this(name, type, null);
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    public DataField copy() {
        return new DataField(name, type.copy(), description);
    }

    public String asSummaryString() {
        return formatString(type.asSummaryString(), true);
    }

    public String asSerializableString() {
        return formatString(type.asSerializableString(), false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataField rowField = (DataField) o;
        return name.equals(rowField.name)
                && type.equals(rowField.type)
                && Objects.equals(description, rowField.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, description);
    }

    private String formatString(String typeString, boolean excludeDescription) {
        if (description == null) {
            return String.format(FIELD_FORMAT_NO_DESCRIPTION, escapeIdentifier(name), typeString);
        } else if (excludeDescription) {
            return String.format(
                    FIELD_FORMAT_WITH_DESCRIPTION, escapeIdentifier(name), typeString, "...");
        } else {
            return String.format(
                    FIELD_FORMAT_WITH_DESCRIPTION,
                    escapeIdentifier(name),
                    typeString,
                    escapeSingleQuotes(description));
        }
    }

    public org.apache.flink.table.api.DataTypes.Field toFlinkDataTypeField() {
        return description == null
                ? org.apache.flink.table.api.DataTypes.FIELD(
                        name, DataTypeUtils.toFlinkDataType(type))
                : org.apache.flink.table.api.DataTypes.FIELD(
                        name, DataTypeUtils.toFlinkDataType(type), description);
    }

    public static DataField fromFlinkDataTypeField(
            org.apache.flink.table.types.logical.RowType.RowField rowField) {
        return DataTypes.FIELD(
                rowField.getName(),
                DataTypeUtils.fromFlinkDataType(
                        LogicalTypeDataTypeConverter.toDataType(rowField.getType())),
                rowField.getDescription().orElse(null));
    }
}
