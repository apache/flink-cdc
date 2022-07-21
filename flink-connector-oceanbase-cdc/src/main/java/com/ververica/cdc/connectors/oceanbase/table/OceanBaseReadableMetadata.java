/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.oceanbase.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.debezium.table.MetadataConverter;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Defines the supported metadata columns for {@link OceanBaseTableSource}. */
public enum OceanBaseReadableMetadata {

    /** Name of the tenant that contains the row. */
    TENANT(
            "tenant_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct value = (Struct) record.value();
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(source.getString("tenant"));
                }
            }),

    /** Name of the database that contains the row. */
    DATABASE(
            "database_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct value = (Struct) record.value();
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(source.getString("database"));
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
                    Struct value = (Struct) record.value();
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(source.getString("table"));
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
                    Struct value = (Struct) record.value();
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    String timestamp = source.getString("timestamp");
                    if (timestamp == null) {
                        timestamp = "0";
                    }
                    return TimestampData.fromEpochMillis(Long.parseLong(timestamp) * 1000);
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
