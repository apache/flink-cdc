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
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data type of a sequence of fields. A field consists of a field name, field type, and an optional
 * description. The most specific type of a record of a table is a record type. In this case, each
 * column of the record corresponds to the field of the record type that has the same ordinal
 * position as the column. Compared to the SQL standard, an optional field description simplifies
 * the handling with complex structures.
 */
@PublicEvolving
public final class RowType extends DataType {

    private static final long serialVersionUID = 1L;

    public static final String FORMAT = "ROW<%s>";

    private final List<DataField> fields;

    public RowType(boolean isNullable, List<DataField> fields) {
        super(isNullable, DataTypeRoot.ROW);
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(fields, "Fields must not be null.")));

        validateFields(fields);
    }

    public RowType(List<DataField> fields) {
        this(true, fields);
    }

    public List<DataField> getFields() {
        return fields;
    }

    public List<String> getFieldNames() {
        return fields.stream().map(DataField::getName).collect(Collectors.toList());
    }

    public List<DataType> getFieldTypes() {
        return fields.stream().map(DataField::getType).collect(Collectors.toList());
    }

    public DataType getTypeAt(int i) {
        return fields.get(i).getType();
    }

    public int getFieldCount() {
        return fields.size();
    }

    public int getFieldIndex(String fieldName) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new RowType(
                isNullable, fields.stream().map(DataField::copy).collect(Collectors.toList()));
    }

    @Override
    public String asSummaryString() {
        return withNullability(
                FORMAT,
                fields.stream().map(DataField::asSummaryString).collect(Collectors.joining(", ")));
    }

    @Override
    public String asSerializableString() {
        return withNullability(
                FORMAT,
                fields.stream()
                        .map(DataField::asSerializableString)
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.unmodifiableList(
                fields.stream().map(DataField::getType).collect(Collectors.toList()));
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        return fields.equals(rowType.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    // --------------------------------------------------------------------------------------------

    private static void validateFields(List<DataField> fields) {
        final List<String> fieldNames =
                fields.stream().map(DataField::getName).collect(Collectors.toList());
        if (fieldNames.stream().anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
            throw new IllegalArgumentException(
                    "Field names must contain at least one non-whitespace character.");
        }
        final Set<String> duplicates =
                fieldNames.stream()
                        .filter(n -> Collections.frequency(fieldNames, n) > 1)
                        .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Field names must be unique. Found duplicates: %s", duplicates));
        }
    }

    public static RowType of(DataType... types) {
        return of(true, types);
    }

    public static RowType of(boolean isNullable, DataType... types) {
        final List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField("f" + i, types[i]));
        }
        return new RowType(isNullable, fields);
    }

    public static RowType of(DataType[] types, String[] names) {
        return of(true, types, names);
    }

    public static RowType of(boolean nullable, DataType[] types, String[] names) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField(names[i], types[i]));
        }
        return new RowType(nullable, fields);
    }

    public static Builder builder() {
        return builder(true);
    }

    public static Builder builder(boolean isNullable) {
        return new Builder(isNullable);
    }

    /** Builder of {@link RowType}. */
    public static class Builder {

        private final List<DataField> fields = new ArrayList<>();

        private final boolean isNullable;

        private Builder(boolean isNullable) {
            this.isNullable = isNullable;
        }

        public Builder field(String name, DataType type) {
            fields.add(new DataField(name, type));
            return this;
        }

        public Builder field(String name, DataType type, String description) {
            fields.add(new DataField(name, type, description));
            return this;
        }

        public Builder fields(List<DataType> types) {
            for (int i = 0; i < types.size(); i++) {
                field("f" + i, types.get(i));
            }
            return this;
        }

        public Builder fields(DataType... types) {
            for (int i = 0; i < types.length; i++) {
                field("f" + i, types[i]);
            }
            return this;
        }

        public Builder fields(DataType[] types, String[] names) {
            for (int i = 0; i < types.length; i++) {
                field(names[i], types[i]);
            }
            return this;
        }

        public RowType build() {
            return new RowType(isNullable, fields);
        }
    }
}
