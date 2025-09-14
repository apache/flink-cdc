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

package org.apache.flink.cdc.runtime.serializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.runtime.serializer.data.ArrayDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.DateDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.DecimalDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.LocalZonedTimestampDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.MapDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.RecordDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.StringDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.TimeDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.TimestampDataSerializer;
import org.apache.flink.cdc.runtime.serializer.data.ZonedTimestampDataSerializer;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** {@link TypeSerializer} of {@link DataType} for internal data structures. */
public class InternalSerializers {
    /**
     * Creates a {@link TypeSerializer} for internal data structures of the given {@link DataType}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeSerializer<T> create(DataType type) {
        return (TypeSerializer<T>) createInternal(type);
    }

    private static TypeSerializer<?> createInternal(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringDataSerializer.INSTANCE;
            case BOOLEAN:
                return BooleanSerializer.INSTANCE;
            case BINARY:
            case VARBINARY:
                return BytePrimitiveArraySerializer.INSTANCE;
            case DECIMAL:
                return new DecimalDataSerializer(getPrecision(type), getScale(type));
            case TINYINT:
                return ByteSerializer.INSTANCE;
            case SMALLINT:
                return ShortSerializer.INSTANCE;
            case INTEGER:
                return IntSerializer.INSTANCE;
            case DATE:
                return DateDataSerializer.INSTANCE;
            case TIME_WITHOUT_TIME_ZONE:
                return TimeDataSerializer.INSTANCE;
            case BIGINT:
                return LongSerializer.INSTANCE;
            case FLOAT:
                return FloatSerializer.INSTANCE;
            case DOUBLE:
                return DoubleSerializer.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampDataSerializer(getPrecision(type));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampDataSerializer(getPrecision(type));
            case TIMESTAMP_WITH_TIME_ZONE:
                return new ZonedTimestampDataSerializer(getPrecision(type));
            case ARRAY:
                return new ArrayDataSerializer(((ArrayType) type).getElementType());
            case ROW:
                return new RecordDataSerializer();
            case MAP:
                MapType mapType = (MapType) type;
                return new MapDataSerializer(mapType.getKeyType(), mapType.getValueType());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type '" + type + "' to get internal serializer");
        }
    }

    private InternalSerializers() {
        // no instantiation
    }
}
