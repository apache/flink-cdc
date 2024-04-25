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

package org.apache.flink.cdc.connectors.oceanbase.table;

import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBaseSourceInfo;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Defines the supported metadata columns for {@link OceanBaseTableSource}. */
public enum OceanBaseReadableMetadata {

    /** Name of the tenant that contains the row. */
    TENANT(
            "tenant_name",
            DataTypes.STRING().nullable(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(OceanBaseSourceInfo.TENANT_KEY));
                }
            }),

    /** Name of the database that contains the row. */
    DATABASE(
            "database_name",
            DataTypes.STRING().nullable(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(OceanBaseSourceInfo.DATABASE_NAME_KEY));
                }
            }),

    /** Name of the schema that contains the row. */
    SCHEMA(
            "schema_name",
            DataTypes.STRING().nullable(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(OceanBaseSourceInfo.SCHEMA_NAME_KEY));
                }
            }),

    /** Name of the table that contains the row. */
    TABLE(
            "table_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(OceanBaseSourceInfo.TABLE_NAME_KEY));
                }
            }),

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the change stream, the value is always 0.
     */
    OP_TS(
            "op_ts",
            DataTypes.TIMESTAMP_LTZ(3).notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return TimestampData.fromEpochMillis(
                            sourceStruct.getInt64(OceanBaseSourceInfo.TIMESTAMP_KEY));
                }
            });

    private final String key;

    private final DataType dataType;

    private final MetadataConverter converter;

    OceanBaseReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public MetadataConverter getConverter() {
        return converter;
    }
}
