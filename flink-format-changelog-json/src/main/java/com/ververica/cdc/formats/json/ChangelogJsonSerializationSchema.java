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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema from Flink Table/SQL internal data structure {@link RowData} to Changelog
 * Json.
 */
public class ChangelogJsonSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = -3999450457829887684L;

    private static final StringData OP_INSERT = StringData.fromString("+I");
    private static final StringData OP_UPDATE_BEFORE = StringData.fromString("-U");
    private static final StringData OP_UPDATE_AFTER = StringData.fromString("+U");
    private static final StringData OP_DELETE = StringData.fromString("-D");

    private final JsonRowDataSerializationSchema jsonSerializer;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    private transient GenericRowData reuse;

    public ChangelogJsonSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
        this.jsonSerializer =
                new JsonRowDataSerializationSchema(
                        createJsonRowType(fromLogicalToDataType(rowType)),
                        timestampFormat,
                        JsonOptions.MapNullKeyMode.FAIL,
                        JsonOptions.MAP_NULL_KEY_LITERAL.defaultValue(),
                        JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER.defaultValue());
        this.timestampFormat = timestampFormat;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.reuse = new GenericRowData(2);
    }

    @Override
    public byte[] serialize(RowData rowData) {
        reuse.setField(0, rowData);
        reuse.setField(1, stringifyRowKind(rowData.getRowKind()));
        return jsonSerializer.serialize(reuse);
    }

    private static StringData stringifyRowKind(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return OP_INSERT;
            case UPDATE_BEFORE:
                return OP_UPDATE_BEFORE;
            case UPDATE_AFTER:
                return OP_UPDATE_AFTER;
            case DELETE:
                return OP_DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operation '" + rowKind + "' for row kind.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangelogJsonSerializationSchema that = (ChangelogJsonSerializationSchema) o;
        return Objects.equals(jsonSerializer, that.jsonSerializer)
                && timestampFormat == that.timestampFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonSerializer, timestampFormat);
    }

    private static RowType createJsonRowType(DataType databaseSchema) {
        DataType payload =
                DataTypes.ROW(
                        DataTypes.FIELD("data", databaseSchema),
                        DataTypes.FIELD("op", DataTypes.STRING()));
        return (RowType) payload.getLogicalType();
    }
}
