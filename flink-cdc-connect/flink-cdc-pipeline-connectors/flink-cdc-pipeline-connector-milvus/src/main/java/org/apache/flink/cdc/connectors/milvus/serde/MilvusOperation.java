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

package org.apache.flink.cdc.connectors.milvus.serde;

import org.apache.flink.cdc.common.event.TableId;

import com.google.gson.JsonObject;

import java.util.Objects;

/** A serialized Milvus write operation. */
public class MilvusOperation {

    public enum Type {
        UPSERT,
        DELETE
    }

    private final Type type;
    private final TableId tableId;
    private final String collectionName;
    private final String partitionName;
    private final JsonObject row;
    private final Object primaryKey;

    private MilvusOperation(
            Type type,
            TableId tableId,
            String collectionName,
            String partitionName,
            JsonObject row,
            Object primaryKey) {
        this.type = Objects.requireNonNull(type);
        this.tableId = Objects.requireNonNull(tableId);
        this.collectionName = Objects.requireNonNull(collectionName);
        this.partitionName = partitionName;
        this.row = row;
        this.primaryKey = primaryKey;
    }

    public static MilvusOperation upsert(
            TableId tableId, String collectionName, String partitionName, JsonObject row) {
        return new MilvusOperation(
                Type.UPSERT,
                tableId,
                collectionName,
                partitionName,
                Objects.requireNonNull(row),
                null);
    }

    public static MilvusOperation delete(
            TableId tableId, String collectionName, String partitionName, Object primaryKey) {
        return new MilvusOperation(
                Type.DELETE,
                tableId,
                collectionName,
                partitionName,
                null,
                Objects.requireNonNull(primaryKey));
    }

    public Type getType() {
        return type;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public JsonObject getRow() {
        return row;
    }

    public Object getPrimaryKey() {
        return primaryKey;
    }
}
