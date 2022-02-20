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

package com.ververica.cdc.connectors.mongodb.source.utils;

import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue;
import com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.json.JsonWriterSettings;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.COPY_KEY_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.HEARTBEAT_TOPIC_NAME;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.JSON_WRITER_SETTINGS_STRICT;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.SOURCE_RECORD_KEY_SCHEMA;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.SOURCE_RECORD_VALUE_SCHEMA;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.TIMESTAMP_KEY_FIELD;
import static java.util.Collections.singletonMap;

/** Utility class to deal record. */
public class MongoRecordUtils {

    private MongoRecordUtils() {}

    /** Check the sourceRecord is snapshot record. */
    public static boolean isSnapshotRecord(SourceRecord sourceRecord) {
        return "true".equals(getOffsetValue(sourceRecord, COPY_KEY_FIELD));
    }

    /** Check the sourceRecord is heartbeat event. */
    public static boolean isHeartbeatEvent(SourceRecord sourceRecord) {
        return "true".equals(getOffsetValue(sourceRecord, MongoDBEnvelope.HEARTBEAT_KEY_FIELD));
    }

    /** Check the sourceRecord is data change record. */
    public static boolean isDataChangeRecord(SourceRecord sourceRecord) {
        return !isWatermarkEvent(sourceRecord) && !isHeartbeatEvent(sourceRecord);
    }

    /** Return the resumeToken from heartbeat event or change stream event. */
    public static BsonDocument getResumeToken(SourceRecord sourceRecord) {
        return BsonDocument.parse(getOffsetValue(sourceRecord, MongoDBEnvelope.ID_FIELD));
    }

    /** Return the documentKey from change stream event. */
    public static BsonDocument getDocumentKey(SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();
        return BsonDocument.parse(value.getString(MongoDBEnvelope.DOCUMENT_KEY_FIELD));
    }

    public static String getOffsetValue(SourceRecord sourceRecord, String key) {
        return (String) sourceRecord.sourceOffset().get(key);
    }

    /** Return the timestamp when the change event is produced in MongoDB. */
    public static Long getMessageTimestamp(SourceRecord sourceRecord) {
        if (isHeartbeatEvent(sourceRecord)) {
            return getMessageTimestampFromHeartbeatEvent(sourceRecord);
        }

        Struct value = (Struct) sourceRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        return source.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    /** Return the timestamp from heartbeat record in MongoDB. */
    public static Long getMessageTimestampFromHeartbeatEvent(SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();
        return value.getInt64(TIMESTAMP_KEY_FIELD);
    }

    /** Return the timestamp when the change event is fetched. */
    public static Long getFetchTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }
        return value.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    /** Return the TableId for snapshot record or change record. */
    public static TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(MongoDBEnvelope.NAMESPACE_FIELD);
        String dbName = source.getString(MongoDBEnvelope.NAMESPACE_DATABASE_FIELD);
        String collName = source.getString(MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD);
        return new TableId(dbName, null, collName);
    }

    public static BsonTimestamp currentBsonTimestamp() {
        return bsonTimestampFromEpochMillis(System.currentTimeMillis());
    }

    public static BsonTimestamp maximumBsonTimestamp() {
        return new BsonTimestamp(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static BsonTimestamp bsonTimestampFromEpochMillis(long epochMillis) {
        return new BsonTimestamp((int) Instant.ofEpochMilli(epochMillis).getEpochSecond(), 1);
    }

    public static SourceRecord createSourceRecord(
            final Map<String, String> partition,
            final Map<String, String> sourceOffset,
            final String topicName,
            final BsonDocument keyDocument,
            final BsonDocument valueDocument) {
        return createSourceRecord(
                partition,
                sourceOffset,
                topicName,
                keyDocument,
                valueDocument,
                JSON_WRITER_SETTINGS_STRICT);
    }

    public static SourceRecord createSourceRecord(
            final Map<String, String> partition,
            final Map<String, String> sourceOffset,
            final String topicName,
            final BsonDocument keyDocument,
            final BsonDocument valueDocument,
            final JsonWriterSettings jsonWriterSettings) {
        BsonValueToSchemaAndValue schemaAndValue =
                new BsonValueToSchemaAndValue(jsonWriterSettings);
        SchemaAndValue keySchemaAndValue =
                schemaAndValue.toSchemaAndValue(SOURCE_RECORD_KEY_SCHEMA, keyDocument);
        SchemaAndValue valueSchemaAndValue =
                schemaAndValue.toSchemaAndValue(SOURCE_RECORD_VALUE_SCHEMA, valueDocument);

        return new SourceRecord(
                partition,
                sourceOffset,
                topicName,
                keySchemaAndValue.schema(),
                keySchemaAndValue.value(),
                valueSchemaAndValue.schema(),
                valueSchemaAndValue.value());
    }

    public static Map<String, String> createSourceOffsetMap(
            final BsonDocument idDocument, boolean isSnapshotRecord) {
        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, idDocument.toJson());
        sourceOffset.put(COPY_KEY_FIELD, String.valueOf(isSnapshotRecord));
        return sourceOffset;
    }

    public static Map<String, String> createPartitionMap(
            String hosts, String database, String collection) {
        StringBuilder builder = new StringBuilder();
        builder.append("mongodb://");
        builder.append(hosts);
        builder.append("/");
        if (StringUtils.isNotEmpty(database)) {
            builder.append(database);
        }
        if (StringUtils.isNotEmpty(collection)) {
            builder.append(".");
            builder.append(collection);
        }
        return singletonMap(NAMESPACE_FIELD, builder.toString());
    }

    public static Map<String, Object> createHeartbeatPartitionMap(String hosts) {
        StringBuilder builder = new StringBuilder();
        builder.append("mongodb://");
        builder.append(hosts);
        builder.append("/");
        builder.append(HEARTBEAT_TOPIC_NAME);
        return singletonMap(NAMESPACE_FIELD, builder.toString());
    }

    public static Map<String, String> createWatermarkPartitionMap(String partition) {
        return singletonMap(NAMESPACE_FIELD, partition);
    }
}
