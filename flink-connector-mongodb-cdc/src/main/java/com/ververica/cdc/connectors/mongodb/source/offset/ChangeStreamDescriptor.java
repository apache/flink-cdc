/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.offset;

import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.regex.Pattern;

/** A structure describes a filter of change stream targets. */
public class ChangeStreamDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String database;

    @Nullable private final String collection;

    @Nullable private final Pattern databaseRegex;

    @Nullable private final Pattern namespaceRegex;

    public ChangeStreamDescriptor(
            @Nullable String database,
            @Nullable String collection,
            @Nullable Pattern databaseRegex,
            @Nullable Pattern namespaceRegex) {
        this.database = database;
        this.collection = collection;
        this.databaseRegex = databaseRegex;
        this.namespaceRegex = namespaceRegex;
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

    public static ChangeStreamDescriptor collection(CollectionId collectionId) {
        return collection(collectionId.getDatabaseName(), collectionId.getCollectionName());
    }

    public static ChangeStreamDescriptor collection(String database, String collection) {
        return new ChangeStreamDescriptor(database, collection, null, null);
    }

    public static ChangeStreamDescriptor database(String database) {
        return new ChangeStreamDescriptor(database, null, null, null);
    }

    public static ChangeStreamDescriptor database(String database, Pattern namespaceRegex) {
        return new ChangeStreamDescriptor(database, null, null, namespaceRegex);
    }

    public static ChangeStreamDescriptor deployment(Pattern databaseRegex) {
        return new ChangeStreamDescriptor(null, null, databaseRegex, null);
    }

    public static ChangeStreamDescriptor deployment(
            @Nullable Pattern databaseRegex, Pattern namespaceRegex) {
        return new ChangeStreamDescriptor(null, null, databaseRegex, namespaceRegex);
    }

    public static ChangeStreamDescriptor deployment() {
        return new ChangeStreamDescriptor(null, null, null, null);
    }
}
