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

package org.apache.flink.cdc.connectors.milvus.utils;

import io.milvus.v2.common.DataType;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Type declaration for a Milvus vector field. */
public class MilvusVectorFieldSpec implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Pattern VECTOR_TYPE_PATTERN =
            Pattern.compile("(?i)\\s*(FloatVector)\\s*\\((\\d+)\\)\\s*");

    private final String fieldName;
    private final DataType dataType;
    private final int dimension;

    public MilvusVectorFieldSpec(String fieldName, DataType dataType, int dimension) {
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Vector field name must not be empty.");
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("Vector dimension must be greater than 0.");
        }
        this.fieldName = MilvusNameUtils.validateIdentifier(fieldName.trim(), "vector field");
        this.dataType = Objects.requireNonNull(dataType);
        this.dimension = dimension;
    }

    public static MilvusVectorFieldSpec parse(String definition) {
        String[] parts = definition.split(":", 2);
        if (parts.length != 2 || parts[0].trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Vector field definition must use field:FloatVector(dim): " + definition);
        }
        Matcher matcher = VECTOR_TYPE_PATTERN.matcher(parts[1]);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Milvus vector field type must use FloatVector(dim): " + definition);
        }
        return new MilvusVectorFieldSpec(
                parts[0].trim(), DataType.FloatVector, Integer.parseInt(matcher.group(2)));
    }

    public String getFieldName() {
        return fieldName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getDimension() {
        return dimension;
    }

    @Override
    public String toString() {
        return fieldName + ":" + dataType.name() + "(" + dimension + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MilvusVectorFieldSpec)) {
            return false;
        }
        MilvusVectorFieldSpec that = (MilvusVectorFieldSpec) o;
        return dimension == that.dimension
                && fieldName.equals(that.fieldName)
                && dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, dataType, dimension);
    }
}
