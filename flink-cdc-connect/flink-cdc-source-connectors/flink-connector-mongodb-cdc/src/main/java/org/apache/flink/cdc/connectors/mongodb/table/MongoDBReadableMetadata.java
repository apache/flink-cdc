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

package org.apache.flink.cdc.connectors.mongodb.table;

import org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataMetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Defines the supported metadata columns for {@link MongoDBTableSource}. */
public enum MongoDBReadableMetadata {

    /** Name of the collection that contain the row. */
    COLLECTION(
            "collection_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct value = (Struct) record.value();
                    Struct to = value.getStruct(MongoDBEnvelope.NAMESPACE_FIELD);
                    return StringData.fromString(
                            to.getString(MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD));
                }
            }),

    /** Name of the database that contain the row. */
    DATABASE(
            "database_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct value = (Struct) record.value();
                    Struct to = value.getStruct(MongoDBEnvelope.NAMESPACE_FIELD);
                    return StringData.fromString(
                            to.getString(MongoDBEnvelope.NAMESPACE_DATABASE_FIELD));
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
                    return TimestampData.fromEpochMillis(
                            (Long) source.get(AbstractSourceInfo.TIMESTAMP_KEY));
                }
            }),

    /**
     * It indicates the row kind of the changelog. '+I' means INSERT message, '-D' means DELETE
     * message, '-U' means UPDATE_BEFORE message and '+U' means UPDATE_AFTER message
     */
    ROW_KIND(
            "row_kind",
            DataTypes.STRING().notNull(),
            new RowDataMetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(RowData rowData) {
                    return StringData.fromString(rowData.getRowKind().shortString());
                }

                @Override
                public Object read(SourceRecord record) {
                    throw new UnsupportedOperationException(
                            "Please call read(RowData rowData) method instead.");
                }
            });

    private final String key;

    private final DataType dataType;

    private final MetadataConverter converter;

    MongoDBReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
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
