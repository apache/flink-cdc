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

package org.apache.flink.cdc.connectors.paimon.sink.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.types.VariantType;

/** Utils for data type conversion between CDC and Paimon System. */
public class TypeUtils {

    /**
     * Convert Flink CDC DataType to Paimon DataType.
     *
     * @param dataType Flink CDC DataType
     * @return Paimon DataType
     */
    public static org.apache.paimon.types.DataType toPaimonDataType(DataType dataType) {
        if (dataType.is(DataTypeRoot.VARIANT)) {
            return new VariantType(dataType.isNullable());
        } else {
            return LogicalTypeConversion.toDataType(
                    DataTypeUtils.toFlinkDataType(dataType).getLogicalType());
        }
    }

    /**
     * Convert Paimon DataType to Flink CDC DataType.
     *
     * @param dataType Paimon DataType
     * @return Flink CDC DataType
     */
    public static DataType toCDCDataType(org.apache.paimon.types.DataType dataType) {
        if (dataType.is(org.apache.paimon.types.DataTypeRoot.VARIANT)) {
            return new org.apache.flink.cdc.common.types.VariantType(dataType.isNullable());
        } else {
            return DataTypeUtils.fromFlinkDataType(
                    TypeConversions.fromLogicalToDataType(
                            LogicalTypeConversion.toLogicalType(dataType)));
        }
    }
}
