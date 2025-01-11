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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;

import java.util.Objects;

/**
 * A wrapper around {@link Event}, which contains the target partition number and will be used in
 * {@link EventPartitioner}.
 */
@Internal
public class PartitioningEvent implements Event {
    private final Event payload;
    private final int sourcePartition;
    private final int targetPartition;

    /**
     * For partitioning events with regular topology, source partition information is not necessary.
     */
    public static PartitioningEvent ofRegular(Event payload, int targetPartition) {
        return new PartitioningEvent(payload, -1, targetPartition);
    }

    /**
     * For distributed topology, we need to track its upstream source subTask ID to correctly
     * distinguish events from different partitions.
     */
    public static PartitioningEvent ofDistributed(
            Event payload, int sourcePartition, int targetPartition) {
        return new PartitioningEvent(payload, sourcePartition, targetPartition);
    }

    private PartitioningEvent(Event payload, int sourcePartition, int targetPartition) {
        this.payload = payload;
        this.sourcePartition = sourcePartition;
        this.targetPartition = targetPartition;
    }

    public Event getPayload() {
        return payload;
    }

    public int getSourcePartition() {
        return sourcePartition;
    }

    public int getTargetPartition() {
        return targetPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitioningEvent that = (PartitioningEvent) o;
        return sourcePartition == that.sourcePartition
                && targetPartition == that.targetPartition
                && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, sourcePartition, targetPartition);
    }

    @Override
    public String toString() {
        return "PartitioningEvent{"
                + "payload="
                + payload
                + ", sourcePartition="
                + sourcePartition
                + ", targetPartition="
                + targetPartition
                + '}';
    }
}
