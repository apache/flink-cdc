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

package com.ververica.cdc.connectors.mongodb.source.split;

/** The state of split to describe the snapshot chunk of MongoDB collection(s). */
public class MongoDBSnapshotSplitState extends MongoDBSplitState {

    public MongoDBSnapshotSplitState(MongoDBSnapshotSplit snapshotSplit) {
        super(snapshotSplit);
    }

    @Override
    public MongoDBSplit toMongoDBSplit() {
        final MongoDBSnapshotSplit snapshotSplit = split.asSnapshotSplit();
        return new MongoDBSnapshotSplit(
                snapshotSplit.splitId(),
                snapshotSplit.getCollectionId(),
                snapshotSplit.getMin(),
                snapshotSplit.getMax(),
                snapshotSplit.getHint(),
                snapshotSplit.isFinished());
    }
}
