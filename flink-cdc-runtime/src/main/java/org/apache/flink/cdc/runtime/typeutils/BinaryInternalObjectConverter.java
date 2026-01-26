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

import org.apache.flink.cdc.common.converter.InternalObjectConverter;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * Also an {@link InternalObjectConverter} but encodes {@link
 * org.apache.flink.cdc.common.data.RecordData} as {@link
 * org.apache.flink.cdc.common.data.binary.BinaryRecordData}.
 */
public class BinaryInternalObjectConverter extends InternalObjectConverter {

    public static Object convertToInternal(Object obj, DataType dataType) {
        if (obj == null) {
            return null;
        }
        if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            List<?> javaObjects = (List<?>) obj;
            List<DataType> dataTypes = rowType.getFieldTypes();
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator((RowType) dataType);
            List<Object> convertedInternalObjects = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                convertedInternalObjects.add(
                        convertToInternal(javaObjects.get(i), dataTypes.get(i)));
            }
            return generator.generate(convertedInternalObjects.toArray());
        }
        return InternalObjectConverter.convertToInternal(obj, dataType);
    }
}
