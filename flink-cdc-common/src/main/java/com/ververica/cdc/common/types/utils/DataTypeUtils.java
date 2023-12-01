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

package com.ververica.cdc.common.types.utils;

import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.MapData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.ZonedTimestampData;
import com.ververica.cdc.common.types.DataType;

/** Utilities for handling {@link DataType}s. */
public class DataTypeUtils {
    /**
     * Returns the conversion class for the given {@link DataType} that is used by the table runtime
     * as internal data structure.
     */
    public static Class<?> toInternalConversionClass(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return DecimalData.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.class;
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedTimestampData.class;
            case ARRAY:
                return ArrayData.class;
            case MAP:
                return MapData.class;
            case ROW:
                return RecordData.class;
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }
}
