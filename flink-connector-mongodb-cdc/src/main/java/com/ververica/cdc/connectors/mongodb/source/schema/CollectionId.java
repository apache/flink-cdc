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

package com.ververica.cdc.connectors.mongodb.source.schema;

import com.mongodb.MongoNamespace;
import io.debezium.schema.DataCollectionId;

import java.util.List;
import java.util.stream.Collectors;

/** Unique identifier for a MongoDB collection. */
public class CollectionId implements DataCollectionId, Comparable<CollectionId> {

    private final String id;
    private final String databaseName;
    private final String collectionName;

    public CollectionId(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.id = databaseName + "." + collectionName;
    }

    public static CollectionId parse(String fullName) {
        return of(new MongoNamespace(fullName));
    }

    public static List<CollectionId> parse(List<String> fullNames) {
        return fullNames.stream().map(CollectionId::parse).collect(Collectors.toList());
    }

    public static CollectionId of(MongoNamespace namespace) {
        return new CollectionId(namespace.getDatabaseName(), namespace.getCollectionName());
    }

    @Override
    public String identifier() {
        return id;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CollectionId) {
            return this.compareTo((CollectionId) obj) == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        return identifier();
    }

    @Override
    public int compareTo(CollectionId that) {
        if (this == that) {
            return 0;
        }
        return this.id.compareTo(that.id);
    }
}
