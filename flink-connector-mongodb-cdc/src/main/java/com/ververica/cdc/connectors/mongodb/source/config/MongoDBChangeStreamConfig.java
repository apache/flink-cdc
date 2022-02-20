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

package com.ververica.cdc.connectors.mongodb.source.config;

import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** A MongoDB Source change stream configuration. */
public class MongoDBChangeStreamConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String database;

    @Nullable private final String collection;

    @Nullable private final Pattern databaseRegex;

    @Nullable private final Pattern namespaceRegex;

    private int batchSize;

    private MongoDBChangeStreamConfig(
            @Nullable String database,
            @Nullable String collection,
            @Nullable Pattern databaseRegex,
            @Nullable Pattern namespaceRegex) {
        this.database = database;
        this.collection = collection;
        this.databaseRegex = databaseRegex;
        this.namespaceRegex = namespaceRegex;
    }

    public static MongoDBChangeStreamConfig collection(CollectionId collectionId) {
        return collection(collectionId.getDatabaseName(), collectionId.getCollectionName());
    }

    public static MongoDBChangeStreamConfig collection(String database, String collection) {
        return new MongoDBChangeStreamConfig(database, collection, null, null);
    }

    public static MongoDBChangeStreamConfig database(String database) {
        return new MongoDBChangeStreamConfig(database, null, null, null);
    }

    public static MongoDBChangeStreamConfig database(String database, Pattern namespaceRegex) {
        return new MongoDBChangeStreamConfig(database, null, null, namespaceRegex);
    }

    public static MongoDBChangeStreamConfig deployment(Pattern databaseRegex) {
        return new MongoDBChangeStreamConfig(null, null, databaseRegex, null);
    }

    public static MongoDBChangeStreamConfig deployment(
            @Nullable Pattern databaseRegex, Pattern namespaceRegex) {
        return new MongoDBChangeStreamConfig(null, null, databaseRegex, namespaceRegex);
    }

    public static MongoDBChangeStreamConfig deployment() {
        return new MongoDBChangeStreamConfig(null, null, null, null);
    }

    @Nullable
    public String getDatabase() {
        return database;
    }

    @Nullable
    public String getCollection() {
        return collection;
    }

    @Nullable
    public Pattern getDatabaseRegex() {
        return databaseRegex;
    }

    @Nullable
    public Pattern getNamespaceRegex() {
        return namespaceRegex;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongoDBChangeStreamConfig)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MongoDBChangeStreamConfig that = (MongoDBChangeStreamConfig) o;
        return Objects.equals(database, that.database)
                && Objects.equals(collection, that.collection)
                && Objects.equals(databaseRegex, that.databaseRegex)
                && Objects.equals(namespaceRegex, that.namespaceRegex)
                && batchSize == that.batchSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, collection, databaseRegex, namespaceRegex, batchSize);
    }

    @Override
    public String toString() {
        return "MongoDBChangeStreamConfig{"
                + "database='"
                + emptyStringIfNull(database)
                + ", collection="
                + emptyStringIfNull(collection)
                + ", databaseRegex="
                + emptyStringIfNull(databaseRegex)
                + ", namespaceRegex="
                + emptyStringIfNull(namespaceRegex)
                + ", batchSize="
                + batchSize
                + '}';
    }

    private Object emptyStringIfNull(Object value) {
        return Optional.ofNullable(value).orElse("''");
    }
}
