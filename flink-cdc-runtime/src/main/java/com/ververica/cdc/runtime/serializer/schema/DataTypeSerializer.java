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

package com.ververica.cdc.runtime.serializer.schema;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.types.ArrayType;
import com.ververica.cdc.common.types.BigIntType;
import com.ververica.cdc.common.types.BinaryType;
import com.ververica.cdc.common.types.BooleanType;
import com.ververica.cdc.common.types.CharType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DateType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.DoubleType;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.MapType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.types.SmallIntType;
import com.ververica.cdc.common.types.TimeType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.TinyIntType;
import com.ververica.cdc.common.types.VarBinaryType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.runtime.serializer.EnumSerializer;

import java.io.IOException;
import java.util.Objects;

/** A {@link TypeSerializer} for {@link DataType}. */
public class DataTypeSerializer extends TypeSerializer<DataType> {

    private static final long serialVersionUID = 1L;

    private final EnumSerializer<DataTypeClass> enumSerializer =
            new EnumSerializer<>(DataTypeClass.class);
    private final RowTypeSerializer rowTypeSerializer = RowTypeSerializer.INSTANCE;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<DataType> duplicate() {
        return new DataTypeSerializer();
    }

    @Override
    public DataType createInstance() {
        return new BigIntType();
    }

    @Override
    public DataType copy(DataType from) {
        if (from instanceof RowType) {
            return rowTypeSerializer.copy((RowType) from);
        }
        return from;
    }

    @Override
    public DataType copy(DataType from, DataType reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(DataType record, DataOutputView target) throws IOException {
        if (record instanceof RowType) {
            enumSerializer.serialize(DataTypeClass.ROW, target);
            rowTypeSerializer.serialize((RowType) record, target);
        } else if (record instanceof BinaryType) {
            enumSerializer.serialize(DataTypeClass.BINARY, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((BinaryType) record).getLength());
        } else if (record instanceof ArrayType) {
            enumSerializer.serialize(DataTypeClass.ARRAY, target);
            target.writeBoolean(record.isNullable());
            this.serialize(((ArrayType) record).getElementType(), target);
        } else if (record instanceof BooleanType) {
            enumSerializer.serialize(DataTypeClass.BOOLEAN, target);
            target.writeBoolean(record.isNullable());
        } else if (record instanceof DecimalType) {
            enumSerializer.serialize(DataTypeClass.DECIMAL, target);
            target.writeBoolean(record.isNullable());

            DecimalType decimalType = (DecimalType) record;
            target.writeInt(decimalType.getPrecision());
            target.writeInt(decimalType.getScale());
        } else if (record instanceof LocalZonedTimestampType) {
            enumSerializer.serialize(DataTypeClass.LOCAL_ZONED_TIMESTAMP, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((LocalZonedTimestampType) record).getPrecision());
        } else if (record instanceof VarBinaryType) {
            enumSerializer.serialize(DataTypeClass.VARBINARY, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((VarBinaryType) record).getLength());
        } else if (record instanceof CharType) {
            enumSerializer.serialize(DataTypeClass.CHAR, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((CharType) record).getLength());
        } else if (record instanceof SmallIntType) {
            enumSerializer.serialize(DataTypeClass.SMALLINT, target);
            target.writeBoolean(record.isNullable());
        } else if (record instanceof TimestampType) {
            enumSerializer.serialize(DataTypeClass.TIMESTAMP, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((TimestampType) record).getPrecision());
        } else if (record instanceof IntType) {
            enumSerializer.serialize(DataTypeClass.INT, target);
            target.writeBoolean(record.isNullable());
        } else if (record instanceof FloatType) {
            enumSerializer.serialize(DataTypeClass.FLOAT, target);
            target.writeBoolean(record.isNullable());
        } else if (record instanceof MapType) {
            enumSerializer.serialize(DataTypeClass.MAP, target);
            target.writeBoolean(record.isNullable());

            MapType mapType = (MapType) record;
            this.serialize(mapType.getKeyType(), target);
            this.serialize(mapType.getValueType(), target);
        } else if (record instanceof TimeType) {
            enumSerializer.serialize(DataTypeClass.TIME, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((TimeType) record).getPrecision());
        } else if (record instanceof TinyIntType) {
            enumSerializer.serialize(DataTypeClass.TINYINT, target);
            target.writeBoolean(record.isNullable());
        } else if (record instanceof VarCharType) {
            enumSerializer.serialize(DataTypeClass.VARCHAR, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((VarCharType) record).getLength());
        } else if (record instanceof DateType) {
            enumSerializer.serialize(DataTypeClass.DATE, target);
            target.writeBoolean(record.isNullable());
        } else if (record instanceof ZonedTimestampType) {
            enumSerializer.serialize(DataTypeClass.ZONED_TIMESTAMP, target);
            target.writeBoolean(record.isNullable());
            target.writeInt(((ZonedTimestampType) record).getPrecision());
        } else if (record instanceof DoubleType) {
            enumSerializer.serialize(DataTypeClass.DOUBLE, target);
            target.writeBoolean(record.isNullable());
        } else if (record instanceof BigIntType) {
            enumSerializer.serialize(DataTypeClass.BIGINT, target);
            target.writeBoolean(record.isNullable());
        } else {
            throw new IllegalArgumentException("Unknown data type : " + record);
        }
    }

    @Override
    public DataType deserialize(DataInputView source) throws IOException {
        DataTypeClass dataTypeClass = enumSerializer.deserialize(source);
        if (dataTypeClass == DataTypeClass.ROW) {
            return rowTypeSerializer.deserialize(source);
        }
        boolean isNullable = source.readBoolean();
        switch (dataTypeClass) {
            case BINARY:
                return new BinaryType(isNullable, source.readInt());
            case ARRAY:
                return new ArrayType(isNullable, this.deserialize(source));
            case BOOLEAN:
                return new BooleanType(isNullable);
            case DECIMAL:
                int precision = source.readInt();
                int scale = source.readInt();
                return new DecimalType(isNullable, precision, scale);
            case LOCAL_ZONED_TIMESTAMP:
                return new LocalZonedTimestampType(isNullable, source.readInt());
            case VARBINARY:
                return new VarBinaryType(isNullable, source.readInt());
            case CHAR:
                return new CharType(isNullable, source.readInt());
            case SMALLINT:
                return new SmallIntType(isNullable);
            case TIMESTAMP:
                return new TimestampType(isNullable, source.readInt());
            case INT:
                return new IntType(isNullable);
            case FLOAT:
                return new FloatType(isNullable);
            case MAP:
                DataType keyType = this.deserialize(source);
                DataType valType = this.deserialize(source);
                return new MapType(isNullable, keyType, valType);
            case TIME:
                return new TimeType(isNullable, source.readInt());
            case TINYINT:
                return new TinyIntType(isNullable);
            case VARCHAR:
                return new VarCharType(isNullable, source.readInt());
            case DATE:
                return new DateType(isNullable);
            case ZONED_TIMESTAMP:
                return new ZonedTimestampType(isNullable, source.readInt());
            case DOUBLE:
                return new DoubleType(isNullable);
            case BIGINT:
                return new BigIntType(isNullable);
            default:
                throw new IllegalArgumentException("Unknown data type : " + dataTypeClass);
        }
    }

    @Override
    public DataType deserialize(DataType reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataTypeSerializer)) {
            return false;
        }
        DataTypeSerializer that = (DataTypeSerializer) o;
        return Objects.equals(enumSerializer, that.enumSerializer)
                && Objects.equals(rowTypeSerializer, that.rowTypeSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enumSerializer, rowTypeSerializer);
    }

    @Override
    public TypeSerializerSnapshot<DataType> snapshotConfiguration() {
        return new DataTypeSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DataTypeSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DataType> {

        public DataTypeSerializerSnapshot() {
            super(DataTypeSerializer::new);
        }
    }

    enum DataTypeClass {
        BINARY,
        ARRAY,
        BOOLEAN,
        DECIMAL,
        LOCAL_ZONED_TIMESTAMP,
        VARBINARY,
        CHAR,
        SMALLINT,
        TIMESTAMP,
        INT,
        ROW,
        FLOAT,
        MAP,
        TIME,
        TINYINT,
        VARCHAR,
        DATE,
        ZONED_TIMESTAMP,
        DOUBLE,
        BIGINT
    }
}
