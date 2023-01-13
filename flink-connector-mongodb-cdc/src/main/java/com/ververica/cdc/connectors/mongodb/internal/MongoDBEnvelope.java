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

package com.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.kafka.connect.source.json.formatter.DefaultJson;
import com.mongodb.kafka.connect.source.schema.AvroSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonValue;
import org.bson.json.JsonWriterSettings;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults.DEFAULT_AVRO_KEY_SCHEMA;

/**
 * An immutable descriptor for the structure of {@link
 * com.mongodb.client.model.changestream.ChangeStreamDocument} envelopes.
 */
public class MongoDBEnvelope {

    public static final String MONGODB_SCHEME = "mongodb";

    public static final String ID_FIELD = "_id";

    public static final String DATA_FIELD = "_data";

    public static final String UUID_FIELD = "uuid";

    public static final String DROPPED_FIELD = "dropped";

    public static final String KEY_FIELD = "key";

    public static final String MAX_FIELD = "max";

    public static final String MIN_FIELD = "min";

    public static final String CLUSTER_TIME_FIELD = "clusterTime";

    public static final String FULL_DOCUMENT_FIELD = "fullDocument";

    public static final String DOCUMENT_KEY_FIELD = "documentKey";

    public static final String OPERATION_TYPE_FIELD = "operationType";

    public static final String NAMESPACE_FIELD = "ns";

    public static final String NAMESPACE_DATABASE_FIELD = "db";

    public static final String NAMESPACE_COLLECTION_FIELD = "coll";

    public static final String COPY_KEY_FIELD = "copy";

    public static final String SNAPSHOT_KEY_FIELD = "snapshot";

    public static final String SOURCE_FIELD = "source";

    public static final String TIMESTAMP_KEY_FIELD = "ts_ms";

    public static final String HEARTBEAT_KEY_FIELD = "HEARTBEAT";

    public static final String HEARTBEAT_TOPIC_NAME = "__mongodb_heartbeats";

    public static final String WATERMARK_TOPIC_NAME = "__mongodb_watermarks";

    // Add "source" and "ts_ms" field to adapt to debezium SourceRecord
    public static final String OUTPUT_SCHEMA =
            "{"
                    + "  \"name\": \"ChangeStream\","
                    + "  \"type\": \"record\","
                    + "  \"fields\": ["
                    + "    { \"name\": \"_id\", \"type\": \"string\" },"
                    + "    { \"name\": \"operationType\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"fullDocument\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"source\","
                    + "      \"type\": [{\"name\": \"source\", \"type\": \"record\", \"fields\": ["
                    + "                {\"name\": \"ts_ms\", \"type\": \"long\"},"
                    + "                {\"name\": \"snapshot\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"ts_ms\", \"type\": [\"long\", \"null\"]},"
                    + "    { \"name\": \"ns\","
                    + "      \"type\": [{\"name\": \"ns\", \"type\": \"record\", \"fields\": ["
                    + "                {\"name\": \"db\", \"type\": \"string\"},"
                    + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"to\","
                    + "      \"type\": [{\"name\": \"to\", \"type\": \"record\",  \"fields\": ["
                    + "                {\"name\": \"db\", \"type\": \"string\"},"
                    + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"documentKey\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"updateDescription\","
                    + "      \"type\": [{\"name\": \"updateDescription\",  \"type\": \"record\", \"fields\": ["
                    + "                 {\"name\": \"updatedFields\", \"type\": [\"string\", \"null\"]},"
                    + "                 {\"name\": \"removedFields\","
                    + "                  \"type\": [{\"type\": \"array\", \"items\": \"string\"}, \"null\"]"
                    + "                  }] }, \"null\"] },"
                    + "    { \"name\": \"clusterTime\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"txnNumber\", \"type\": [\"long\", \"null\"]},"
                    + "    { \"name\": \"lsid\", \"type\": [{\"name\": \"lsid\", \"type\": \"record\","
                    + "               \"fields\": [ {\"name\": \"id\", \"type\": \"string\"},"
                    + "                             {\"name\": \"uid\", \"type\": \"string\"}] }, \"null\"] }"
                    + "  ]"
                    + "}";

    public static final Schema HEARTBEAT_VALUE_SCHEMA =
            SchemaBuilder.struct().field(TIMESTAMP_KEY_FIELD, Schema.INT64_SCHEMA).build();

    public static final Schema SOURCE_RECORD_KEY_SCHEMA =
            AvroSchema.fromJson(DEFAULT_AVRO_KEY_SCHEMA);

    public static final Schema SOURCE_RECORD_VALUE_SCHEMA = AvroSchema.fromJson(OUTPUT_SCHEMA);

    public static final JsonWriterSettings JSON_WRITER_SETTINGS_STRICT =
            new DefaultJson().getJsonWriterSettings();

    public static final BsonDocument ID_INDEX = new BsonDocument(ID_FIELD, new BsonInt32(1));

    public static final BsonValue BSON_MIN_KEY = new BsonMinKey();

    public static final BsonValue BSON_MAX_KEY = new BsonMaxKey();

    public static String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
