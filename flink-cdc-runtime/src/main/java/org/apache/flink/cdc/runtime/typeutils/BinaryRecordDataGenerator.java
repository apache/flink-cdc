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

package org.apache.flink.cdc.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.serializer.InternalSerializers;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryRecordDataWriter;
import org.apache.flink.cdc.runtime.serializer.data.writer.BinaryWriter;

import java.util.Arrays;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

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
                        .toArray(TypeSerializer[]::new));
    }

    public BinaryRecordDataGenerator(DataType[] dataTypes, TypeSerializer[] serializers) {
        checkArgument(
                dataTypes.length == serializers.length,
                "The types and serializers must have the same length. But types is %s and serializers is %s",
                dataTypes.length,
                serializers.length);

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
                "The types and values must have the same length. But types is %s and values is %s",
                dataTypes.length,
                rowFields.length);

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
