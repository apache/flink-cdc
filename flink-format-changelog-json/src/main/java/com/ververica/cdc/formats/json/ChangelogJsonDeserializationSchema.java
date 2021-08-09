/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Deserialization schema from Changelog Json to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public class ChangelogJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = -2084214292622004460L;

    /** The deserializer to deserialize Debezium JSON data. */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    public ChangelogJsonDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormatOption) {
        this.resultTypeInfo = resultTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        createJsonRowType(fromLogicalToDataType(rowType)),
                        // the result type is never used, so it's fine to pass in Debezium's result
                        // type
                        resultTypeInfo,
                        false, // ignoreParseErrors already contains the functionality of
                        // failOnMissingField
                        ignoreParseErrors,
                        timestampFormatOption);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] bytes, Collector<RowData> out) throws IOException {
        try {
            GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(bytes);
            GenericRowData data = (GenericRowData) row.getField(0);
            String op = row.getString(1).toString();
            RowKind rowKind = parseRowKind(op);
            data.setRowKind(rowKind);
            out.collect(data);
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt Changelog JSON message '%s'.", new String(bytes)), t);
            }
        }
    }

    private static RowKind parseRowKind(String op) {
        switch (op) {
            case "+I":
                return RowKind.INSERT;
            case "-U":
                return RowKind.UPDATE_BEFORE;
            case "+U":
                return RowKind.UPDATE_AFTER;
            case "-D":
                return RowKind.DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operation '" + op + "' for row kind.");
        }
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangelogJsonDeserializationSchema that = (ChangelogJsonDeserializationSchema) o;
        return ignoreParseErrors == that.ignoreParseErrors
                && Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && Objects.equals(resultTypeInfo, that.resultTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonDeserializer, resultTypeInfo, ignoreParseErrors);
    }

    private static RowType createJsonRowType(DataType databaseSchema) {
        DataType payload =
                DataTypes.ROW(
                        DataTypes.FIELD("data", databaseSchema),
                        DataTypes.FIELD("op", DataTypes.STRING()));
        return (RowType) payload.getLogicalType();
    }
}
