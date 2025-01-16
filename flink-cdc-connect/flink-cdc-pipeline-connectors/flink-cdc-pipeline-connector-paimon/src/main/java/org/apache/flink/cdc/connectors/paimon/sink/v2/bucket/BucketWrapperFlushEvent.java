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

package org.apache.flink.cdc.connectors.paimon.sink.v2.bucket;

import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;

import java.util.List;
import java.util.Objects;

/** A wrapper class for {@link FlushEvent} to attach bucket id. */
public class BucketWrapperFlushEvent extends FlushEvent implements BucketWrapper {

    private final int bucket;
    private final int bucketAssignTaskId;

    public BucketWrapperFlushEvent(
            int bucket,
            int sourceSubTaskId,
            int bucketAssignTaskId,
            List<TableId> tableIds,
            SchemaChangeEventType schemaChangeEventType) {
        super(sourceSubTaskId, tableIds, schemaChangeEventType);
        this.bucket = bucket;
        this.bucketAssignTaskId = bucketAssignTaskId;
    }

    public int getBucketAssignTaskId() {
        return bucketAssignTaskId;
    }

    @Override
    public int getBucket() {
        return bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BucketWrapperFlushEvent that = (BucketWrapperFlushEvent) o;
        return bucket == that.bucket
                && bucketAssignTaskId == that.bucketAssignTaskId
                && getSourceSubTaskId() == that.getSourceSubTaskId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucket, bucketAssignTaskId);
    }

    @Override
    public String toString() {
        return "BucketWrapperFlushEvent{subTaskId="
                + getSourceSubTaskId()
                + ", bucketAssignTaskId="
                + bucketAssignTaskId
                + ", bucket="
                + bucket
                + '}';
    }
}
