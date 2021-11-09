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

package com.ververica.cdc.connectors.mongodb.internal;

/**
 * An immutable descriptor for the structure of {@link
 * com.mongodb.client.model.changestream.ChangeStreamDocument} envelopes.
 */
public class MongoDBEnvelope {

    public static final String CLUSTER_TIME_FIELD = "clusterTime";

    public static final String FULL_DOCUMENT_FIELD = "fullDocument";

    public static final String DOCUMENT_KEY_FIELD = "documentKey";

    public static final String OPERATION_TYPE_FIELD = "operationType";

    public static final String NAMESPACE_FIELD = "ns";

    public static final String NAMESPACE_DATABASE_FIELD = "db";

    public static final String NAMESPACE_COLLECTION_FIELD = "coll";

    public static final String COPY_KEY_FIELD = "copy";

    public static final String HEARTBEAT_KEY_FIELD = "HEARTBEAT";
}
