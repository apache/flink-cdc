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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.mongodb.client.model.changestream.OperationType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;

import java.time.ZoneId;

/**
 * Deserialization schema from Mongodb ChangeStreamDocument to Flink Table/SQL internal data that
 * produces full changelog mode structure {@link RowData}.
 */
@PublicEvolving
public class MongoDBConnectorFullChangelogDeserializationSchema
        extends MongoDBConnectorDeserializationSchema {

    private static final long serialVersionUID = 1750787080613035184L;

    public MongoDBConnectorFullChangelogDeserializationSchema(
            RowType physicalDataType,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> resultTypeInfo,
            ZoneId localTimeZone) {
        super(physicalDataType, metadataConverters, resultTypeInfo, localTimeZone);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();

        OperationType op = operationTypeFor(record);

        BsonDocument documentKey =
                extractBsonDocument(value, valueSchema, MongoDBEnvelope.DOCUMENT_KEY_FIELD);
        BsonDocument fullDocument =
                extractBsonDocument(value, valueSchema, MongoDBEnvelope.FULL_DOCUMENT_FIELD);

        BsonDocument fullDocumentBeforeChange =
                extractBsonDocument(
                        value, valueSchema, MongoDBEnvelope.FULL_DOCUMENT_BEFORE_CHANGE_FIELD);

        switch (op) {
            case INSERT:
                GenericRowData insert = extractRowData(fullDocument);
                insert.setRowKind(RowKind.INSERT);
                emit(record, insert, out);
                break;
            case DELETE:
                // there might be FullDocBeforeChange field
                // if fullDocumentPrePostImage feature is on
                // convert it to Delete row data with full document
                if (fullDocumentBeforeChange != null) {
                    GenericRowData updateBefore = extractRowData(fullDocumentBeforeChange);
                    updateBefore.setRowKind(RowKind.DELETE);
                    emit(record, updateBefore, out);
                } else {
                    GenericRowData delete = extractRowData(documentKey);
                    delete.setRowKind(RowKind.DELETE);
                    emit(record, delete, out);
                }
                break;
            case UPDATE:
                // It’s null if another operation deletes the document
                // before the lookup operation happens. Ignored it.
                if (fullDocument == null) {
                    break;
                }
                // there might be FullDocBeforeChange field
                // if fullDocumentPrePostImage feature is on
                // convert it to UB row data
                if (fullDocumentBeforeChange != null) {
                    GenericRowData updateBefore = extractRowData(fullDocumentBeforeChange);
                    updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
                    emit(record, updateBefore, out);
                }
                GenericRowData updateAfter = extractRowData(fullDocument);
                updateAfter.setRowKind(RowKind.UPDATE_AFTER);
                emit(record, updateAfter, out);
                break;
            case REPLACE:
                // there might be FullDocBeforeChange field
                // if fullDocumentPrePostImage feature is on
                // convert it to UB row data
                if (fullDocumentBeforeChange != null) {
                    GenericRowData updateBefore = extractRowData(fullDocumentBeforeChange);
                    updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
                    emit(record, updateBefore, out);
                }
                GenericRowData replaceAfter = extractRowData(fullDocument);
                replaceAfter.setRowKind(RowKind.UPDATE_AFTER);
                emit(record, replaceAfter, out);
                break;
            case INVALIDATE:
            case DROP:
            case DROP_DATABASE:
            case RENAME:
            case OTHER:
            default:
                break;
        }
    }
}
