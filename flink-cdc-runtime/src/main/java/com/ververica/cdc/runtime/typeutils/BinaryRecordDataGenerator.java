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

package com.ververica.cdc.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.serializer.InternalSerializers;
import com.ververica.cdc.runtime.serializer.NullableSerializerWrapper;
import com.ververica.cdc.runtime.serializer.data.writer.BinaryRecordDataWriter;
import com.ververica.cdc.runtime.serializer.data.writer.BinaryWriter;

import java.util.Arrays;

import static com.ververica.cdc.common.utils.Preconditions.checkArgument;

/** This class is used to create {@link BinaryRecordData}. */
@PublicEvolving
public class BinaryRecordDataGenerator {

    private final DataType[] dataTypes;
    private final TypeSerializer[] serializers;

    private transient BinaryRecordData reuseRecordData;
    private transient BinaryRecordDataWriter reuseWriter;

    public BinaryRecordDataGenerator(RowType recordType) {
        this(recordType.getChildren().toArray(new DataType[0]));
    }

    public BinaryRecordDataGenerator(DataType[] dataTypes) {
        this(
                dataTypes,
                Arrays.stream(dataTypes)
                        .map(InternalSerializers::create)
                        .map(NullableSerializerWrapper::new)
                        .toArray(TypeSerializer[]::new));
    }

    public BinaryRecordDataGenerator(DataType[] dataTypes, TypeSerializer[] serializers) {
        checkArgument(
                dataTypes.length == serializers.length,
                String.format(
                        "The types and serializers must have the same length. But types is %d and serializers is %d",
                        dataTypes.length, serializers.length));

        this.dataTypes = dataTypes;
        this.serializers = serializers;

        this.reuseRecordData = new BinaryRecordData(dataTypes.length);
        this.reuseWriter = new BinaryRecordDataWriter(reuseRecordData);
    }

    /**
     * Creates an instance of {@link BinaryRecordData} with given field values.
     *
     * <p>Note: All fields of the record must be internal data structures. See {@link RecordData}.
     */
    public BinaryRecordData generate(Object[] rowFields) {
        checkArgument(
                dataTypes.length == rowFields.length,
                String.format(
                        "The types and values must have the same length. But types is %d and values is %d",
                        dataTypes.length, rowFields.length));

        reuseWriter.reset();
        for (int i = 0; i < dataTypes.length; i++) {
            if (rowFields[i] == null) {
                reuseWriter.setNullAt(i);
            } else {
                BinaryWriter.write(reuseWriter, i, rowFields[i], dataTypes[i], serializers[i]);
            }
        }
        reuseWriter.complete();
        return reuseRecordData.copy();
    }
}
