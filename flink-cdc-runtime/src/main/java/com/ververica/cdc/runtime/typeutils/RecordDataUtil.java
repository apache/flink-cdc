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

/** Utils to create {@link BinaryRecordData}. */
@PublicEvolving
public class RecordDataUtil {

    /**
     * Creates an instance of {@link BinaryRecordData} with given field values and {@link RowType}.
     *
     * <p>Note: All fields of the record must be internal data structures. See {@link RecordData}.
     */
    public static BinaryRecordData of(RowType recordType, Object[] rowFields) {
        return of(recordType.getChildren().toArray(new DataType[0]), rowFields);
    }

    /**
     * Creates an instance of {@link BinaryRecordData} with given field values and {@link
     * DataType}s.
     *
     * <p>Note: All fields of the record must be internal data structures. See {@link RecordData}.
     */
    public static BinaryRecordData of(DataType[] types, Object[] values) {
        checkArgument(
                types.length == values.length,
                String.format(
                        "The types and values must have the same length. But types is %d and values is %d",
                        types.length, values.length));

        TypeSerializer[] serializers =
                Arrays.stream(types)
                        .map(InternalSerializers::create)
                        .map(NullableSerializerWrapper::new)
                        .toArray(TypeSerializer[]::new);

        BinaryRecordData recordData = new BinaryRecordData(types.length);
        BinaryRecordDataWriter writer = new BinaryRecordDataWriter(recordData);
        writer.reset();
        for (int i = 0; i < types.length; i++) {
            if (values[i] == null) {
                writer.setNullAt(i);
            } else {
                BinaryWriter.write(writer, i, values[i], types[i], serializers[i]);
            }
        }
        writer.complete();
        return recordData;
    }
}
