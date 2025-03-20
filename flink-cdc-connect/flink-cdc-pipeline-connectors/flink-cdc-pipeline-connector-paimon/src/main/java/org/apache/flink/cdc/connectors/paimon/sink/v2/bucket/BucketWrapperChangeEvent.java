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

import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.TableId;

import java.io.Serializable;
import java.util.Objects;

/** A wrapper class for {@link ChangeEvent} to attach bucket id. */
public class BucketWrapperChangeEvent implements ChangeEvent, BucketWrapper, Serializable {
    private static final long serialVersionUID = 1L;
    private final int bucket;

    private final ChangeEvent innerEvent;

    public BucketWrapperChangeEvent(int bucket, ChangeEvent innerEvent) {
        this.bucket = bucket;
        this.innerEvent = innerEvent;
    }

    public int getBucket() {
        return bucket;
    }

    public ChangeEvent getInnerEvent() {
        return innerEvent;
    }

    @Override
    public TableId tableId() {
        return innerEvent.tableId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketWrapperChangeEvent that = (BucketWrapperChangeEvent) o;
        return bucket == that.bucket && Objects.equals(innerEvent, that.innerEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, innerEvent);
    }

    @Override
    public String toString() {
        return "BucketWrapperChangeEvent{"
                + "bucket="
                + bucket
                + ", innerEvent="
                + innerEvent
                + '}';
    }
}
