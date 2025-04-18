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

package org.apache.flink.cdc.runtime.serializer.schema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/** A test for the {@link DataTypeSerializer}. */
class DataTypeSerializerTest extends SerializerTestBase<DataType> {

    private static final Logger LOG = LoggerFactory.getLogger(DataTypeSerializerTest.class);

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

    @Test
    void testNestedRow() throws IOException {
        RowType innerMostRowType = RowType.of(DataTypes.BIGINT());
        RowType outerRowType =
                RowType.of(DataTypes.ROW(DataTypes.FIELD("outerRow", innerMostRowType)));

        DataTypeSerializer serializer = new DataTypeSerializer();

        // Log to ensure INSTANCE is initialized
        Assertions.assertThat(RowTypeSerializer.INSTANCE)
                .as("RowTypeSerializer.INSTANCE should not be null")
                .isNotNull();
        LOG.info("RowTypeSerializer.INSTANCE is initialized");

        // Copy the RowType
        RowType copiedRow = (RowType) serializer.copy(outerRowType);
        Assertions.assertThat(copiedRow).as("Copied RowType should not be null").isNotNull();
        LOG.info("Copied RowType: {}", copiedRow);

        // Serialize the RowType
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputView outputView = new DataOutputViewStreamWrapper(byteArrayOutputStream);
        serializer.serialize(outerRowType, outputView);

        // Deserialize the RowType
        ByteArrayInputStream byteArrayInputStream =
                new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        DataInputView inputView = new DataInputViewStreamWrapper(byteArrayInputStream);
        RowType deserializedRow = (RowType) serializer.deserialize(inputView);

        // Assert that the deserialized RowType is not null and equals the original
        Assertions.assertThat(deserializedRow)
                .as("Deserialized RowType should not be null")
                .isNotNull()
                .as("Deserialized RowType should match the original")
                .isEqualTo(outerRowType);
        LOG.info("Deserialized RowType: {}", deserializedRow);
    }
}
