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

package org.apache.flink.cdc.connectors.tdengine.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkConfig;

/** Flink CDC to TDengine type mapping. */
public class TDengineTypeUtils {

    private TDengineTypeUtils() {}

    public static String toTDengineType(
            DataType dataType, boolean timestampField, TDengineDataSinkConfig config) {
        if (timestampField) {
            if (!isTimestampSourceType(dataType)) {
                throw new UnsupportedOperationException(
                        "TDengine timestamp.field must be DATE, TIME, TIMESTAMP, BIGINT, CHAR or VARCHAR, but was "
                                + dataType.asSummaryString()
                                + ".");
            }
            return "TIMESTAMP";
        }

        DataTypeRoot root = dataType.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return "BOOL";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "double".equals(config.getDecimalMapping())
                        ? "DOUBLE"
                        : varcharType(dataType, config);
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return varcharType(dataType, config);
            case BINARY:
            case VARBINARY:
                return "VARBINARY(" + resolveBinaryLength(dataType, config) + ")";
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink CDC type for TDengine sink: "
                                + dataType.asSummaryString());
        }
    }

    public static boolean isTimestampSourceType(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BIGINT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case CHAR:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    public static int resolveStringLength(DataType dataType, int defaultLength) {
        if (dataType instanceof org.apache.flink.cdc.common.types.CharType) {
            return ((org.apache.flink.cdc.common.types.CharType) dataType).getLength();
        }
        if (dataType instanceof VarCharType) {
            int length = ((VarCharType) dataType).getLength();
            return length == VarCharType.MAX_LENGTH
                    ? defaultLength
                    : Math.min(length, defaultLength);
        }
        return defaultLength;
    }

    public static int resolveBinaryLength(DataType dataType, TDengineDataSinkConfig config) {
        int defaultLength = config.getVarcharMaxLengthDefault();
        if (dataType instanceof org.apache.flink.cdc.common.types.BinaryType) {
            return Math.min(
                    ((org.apache.flink.cdc.common.types.BinaryType) dataType).getLength(),
                    defaultLength);
        }
        if (dataType instanceof VarBinaryType) {
            int length = ((VarBinaryType) dataType).getLength();
            return length == VarBinaryType.MAX_LENGTH
                    ? defaultLength
                    : Math.min(length, defaultLength);
        }
        return defaultLength;
    }

    private static String varcharType(DataType dataType, TDengineDataSinkConfig config) {
        return config.getStringType().toUpperCase()
                + "("
                + resolveStringLength(dataType, config.getVarcharMaxLengthDefault())
                + ")";
    }
}
