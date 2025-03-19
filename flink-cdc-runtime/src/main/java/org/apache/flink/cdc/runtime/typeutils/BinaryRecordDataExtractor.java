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

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.apache.flink.shaded.guava31.com.google.common.io.BaseEncoding;

import javax.annotation.CheckReturnValue;

import java.util.ArrayList;
import java.util.List;

/** Utility methods for extracting human-readable objects from {@link RecordData}. */
public class BinaryRecordDataExtractor {

    /** Converts a binary record data with specific schema to Java objects. */
    @CheckReturnValue
    public static Object extractRecord(RecordData record, Schema schema) {
        return extractRecord(record, schema.toRowDataType());
    }

    /** Converts a generic binary record data to Java objects. */
    @CheckReturnValue
    public static Object extractRecord(Object object, DataType dataType) {
        if (object == null) {
            return "null";
        }
        if (dataType instanceof BinaryType || dataType instanceof VarBinaryType) {
            Preconditions.checkArgument(
                    object instanceof byte[],
                    "Column data of BinaryType and VarBinaryType should be `byte[]`, but was %s",
                    object.getClass().getName());
            return BaseEncoding.base64().encode((byte[]) object);
        } else if (dataType instanceof MapType) {
            Preconditions.checkArgument(
                    object instanceof MapData,
                    "Column data of MapType should be MapData, but was %s",
                    object.getClass().getName());
            MapType mapType = (MapType) dataType;
            MapData mapData = (MapData) object;
            List<?> keyArray =
                    (List<?>)
                            extractRecord(
                                    mapData.keyArray(), DataTypes.ARRAY(mapType.getKeyType()));
            List<?> valueArray =
                    (List<?>)
                            extractRecord(
                                    mapData.valueArray(), DataTypes.ARRAY(mapType.getValueType()));
            Preconditions.checkArgument(
                    keyArray.size() == valueArray.size(),
                    "Malformed MapData: keyArray size (%d) differs from valueArray (%d)",
                    keyArray.size(),
                    valueArray.size());
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < keyArray.size(); i++) {
                sb.append(keyArray.get(i)).append(" -> ").append(valueArray.get(i)).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            return sb.append("}").toString();
        } else if (dataType instanceof ArrayType) {
            Preconditions.checkArgument(
                    object instanceof ArrayData,
                    "Column data of ArrayType should be ArrayData, but was %s",
                    object.getClass().getName());
            ArrayType arrayType = (ArrayType) dataType;
            ArrayData arrayData = (ArrayData) object;
            ArrayData.ElementGetter getter =
                    ArrayData.createElementGetter(arrayType.getElementType());
            List<Object> results = new ArrayList<>();
            for (int i = 0; i < arrayData.size(); i++) {
                results.add(getter.getElementOrNull(arrayData, i));
            }
            return results;
        } else if (dataType instanceof RowType) {
            Preconditions.checkArgument(
                    object instanceof RecordData,
                    "Column data of RowType should be RecordData, but was %s",
                    object.getClass().getName());
            RowType rowType = (RowType) dataType;
            RecordData binaryRecordData = (RecordData) object;
            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getFieldTypes();
            List<RecordData.FieldGetter> fieldGetters =
                    SchemaUtils.createFieldGetters(fieldTypes.toArray(new DataType[0]));
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                sb.append(fieldNames.get(i))
                        .append(": ")
                        .append(fieldTypes.get(i))
                        .append(" -> ")
                        .append(
                                extractRecord(
                                        fieldGetters.get(i).getFieldOrNull(binaryRecordData),
                                        fieldTypes.get(i)))
                        .append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            return sb.append("}").toString();
        } else {
            return object.toString();
        }
    }
}
