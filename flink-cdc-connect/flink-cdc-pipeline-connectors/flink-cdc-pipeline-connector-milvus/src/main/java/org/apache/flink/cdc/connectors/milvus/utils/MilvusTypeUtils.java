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

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;

import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;

import java.util.List;
import java.util.Optional;

/** Flink CDC to Milvus type mapping. */
public class MilvusTypeUtils {

    private MilvusTypeUtils() {}

    public static Optional<MilvusVectorFieldSpec> findVectorFieldSpec(
            String columnName, List<MilvusVectorFieldSpec> vectorFields) {
        return vectorFields.stream()
                .filter(spec -> spec.getFieldName().equals(columnName))
                .findFirst();
    }

    public static AddFieldReq toAddFieldReq(
            Column column,
            boolean primaryKey,
            Optional<MilvusVectorFieldSpec> vectorField,
            int varcharMaxLength) {
        AddFieldReq.AddFieldReqBuilder<?> builder =
                AddFieldReq.builder()
                        .fieldName(MilvusNameUtils.validateIdentifier(column.getName(), "field"))
                        .isPrimaryKey(primaryKey)
                        .autoID(false)
                        .isNullable(!primaryKey && column.getType().isNullable());
        if (vectorField.isPresent()) {
            MilvusVectorFieldSpec spec = vectorField.get();
            return builder.dataType(spec.getDataType()).dimension(spec.getDimension()).build();
        }

        DataType milvusType = toMilvusType(column.getType(), primaryKey);
        builder.dataType(milvusType);
        if (milvusType == DataType.VarChar) {
            builder.maxLength(resolveVarcharMaxLength(column.getType(), varcharMaxLength));
        }
        return builder.build();
    }

    public static DataType toMilvusType(org.apache.flink.cdc.common.types.DataType dataType) {
        return toMilvusType(dataType, false);
    }

    public static DataType toMilvusType(
            org.apache.flink.cdc.common.types.DataType dataType, boolean primaryKey) {
        DataTypeRoot root = dataType.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return DataType.Bool;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return DataType.Int32;
            case BIGINT:
                return DataType.Int64;
            case FLOAT:
                return DataType.Float;
            case DOUBLE:
                return DataType.Double;
            case CHAR:
            case VARCHAR:
            case DECIMAL:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataType.VarChar;
            case ARRAY:
                if (primaryKey) {
                    throw new UnsupportedOperationException("Milvus primary key cannot be ARRAY.");
                }
                throw new UnsupportedOperationException(
                        "ARRAY is only supported when configured as a Milvus vector field.");
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink CDC type for Milvus sink: "
                                + dataType.asSummaryString());
        }
    }

    public static boolean isSupportedPrimaryKeyType(
            org.apache.flink.cdc.common.types.DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BIGINT:
            case CHAR:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    public static boolean isSupportedVectorSourceType(
            org.apache.flink.cdc.common.types.DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                return dataType.getChildren().size() == 1
                        && (dataType.getChildren().get(0) instanceof FloatType
                                || dataType.getChildren().get(0) instanceof DoubleType);
            case CHAR:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    public static int resolveVarcharMaxLength(
            org.apache.flink.cdc.common.types.DataType dataType, int defaultLength) {
        if (dataType instanceof org.apache.flink.cdc.common.types.VarCharType) {
            int length = ((org.apache.flink.cdc.common.types.VarCharType) dataType).getLength();
            return length == org.apache.flink.cdc.common.types.VarCharType.MAX_LENGTH
                    ? defaultLength
                    : Math.min(length, defaultLength);
        }
        if (dataType instanceof org.apache.flink.cdc.common.types.CharType) {
            return ((org.apache.flink.cdc.common.types.CharType) dataType).getLength();
        }
        return defaultLength;
    }
}
