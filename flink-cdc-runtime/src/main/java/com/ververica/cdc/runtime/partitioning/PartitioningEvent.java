/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.runtime.partitioning;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.Event;

import java.util.Objects;

/**
 * A wrapper around {@link Event}, which contains the target partition number and will be used in
 * {@link EventPartitioner}.
 */
@Internal
public class PartitioningEvent implements Event {
    private final Event payload;
    private final int targetPartition;

    public PartitioningEvent(Event payload, int targetPartition) {
        this.payload = payload;
        this.targetPartition = targetPartition;
    }

    public Event getPayload() {
        return payload;
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
        return targetPartition == that.targetPartition && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, targetPartition);
    }

    @Override
    public String toString() {
        return "PartitioningEvent{"
                + "payload="
                + payload
                + ", targetPartition="
                + targetPartition
                + '}';
    }
}
