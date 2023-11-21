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

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.serializer.SerializerTestBase;

import java.util.Arrays;
import java.util.stream.Stream;

/** A test for the {@link DataTypeSerializer}. */
public class DataTypeSerializerTest extends SerializerTestBase<DataType> {
    @Override
    protected TypeSerializer<DataType> createSerializer() {
        return new DataTypeSerializer();
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<DataType> getTypeClass() {
        return DataType.class;
    }

    @Override
    protected DataType[] getTestData() {
        DataType[] allTypes =
                new DataType[] {
                    DataTypes.BOOLEAN(),
                    DataTypes.BYTES(),
                    DataTypes.BINARY(10),
                    DataTypes.VARBINARY(10),
                    DataTypes.CHAR(10),
                    DataTypes.VARCHAR(10),
                    DataTypes.STRING(),
                    DataTypes.INT(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.BIGINT(),
                    DataTypes.DOUBLE(),
                    DataTypes.FLOAT(),
                    DataTypes.DECIMAL(6, 3),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIME(6),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP_LTZ(),
                    DataTypes.TIMESTAMP_LTZ(6),
                    DataTypes.TIMESTAMP_TZ(),
                    DataTypes.TIMESTAMP_TZ(6),
                    DataTypes.ARRAY(DataTypes.BIGINT()),
                    DataTypes.MAP(DataTypes.SMALLINT(), DataTypes.STRING()),
                    DataTypes.ROW(
                            DataTypes.FIELD("f1", DataTypes.STRING()),
                            DataTypes.FIELD("f2", DataTypes.STRING(), "desc")),
                    DataTypes.ROW(DataTypes.SMALLINT(), DataTypes.STRING())
                };
        return Stream.concat(
                        Arrays.stream(allTypes), Arrays.stream(allTypes).map(DataType::notNull))
                .toArray(DataType[]::new);
    }
}
