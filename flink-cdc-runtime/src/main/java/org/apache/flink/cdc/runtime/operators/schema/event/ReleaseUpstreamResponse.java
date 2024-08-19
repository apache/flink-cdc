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

package org.apache.flink.cdc.runtime.operators.schema.event;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The response for {@link ReleaseUpstreamRequest} from {@link SchemaRegistry} to {@link
 * SchemaOperator}.
 */
public class ReleaseUpstreamResponse implements CoordinationResponse {

    private static final long serialVersionUID = 1L;

    /**
     * Whether the SchemaOperator need to buffer data and the SchemaOperatorCoordinator need to wait
     * for flushing.
     */
    private final List<SchemaChangeEvent> finishedSchemaChangeEvents;

    private final List<Tuple2<SchemaChangeEvent, Throwable>> failedSchemaChangeEvents;

    private final List<SchemaChangeEvent> ignoredSchemaChangeEvents;

    public ReleaseUpstreamResponse(
            List<SchemaChangeEvent> finishedSchemaChangeEvents,
            List<Tuple2<SchemaChangeEvent, Throwable>> failedSchemaChangeEvents,
            List<SchemaChangeEvent> ignoredSchemaChangeEvents) {
        this.finishedSchemaChangeEvents = finishedSchemaChangeEvents;
        this.failedSchemaChangeEvents = failedSchemaChangeEvents;
        this.ignoredSchemaChangeEvents = ignoredSchemaChangeEvents;
    }

    public List<SchemaChangeEvent> getFinishedSchemaChangeEvents() {
        return finishedSchemaChangeEvents;
    }

    public List<Tuple2<SchemaChangeEvent, Throwable>> getFailedSchemaChangeEvents() {
        return failedSchemaChangeEvents;
    }

    public List<SchemaChangeEvent> getIgnoredSchemaChangeEvents() {
        return ignoredSchemaChangeEvents;
    }

    public String getPrintableFailedSchemaChangeEvents() {
        return failedSchemaChangeEvents.stream()
                .map(e -> "Failed to apply " + e.f0 + ". Caused by: " + e.f1)
                .collect(Collectors.joining("\n"));
    }

    public boolean hasException() {
        return !failedSchemaChangeEvents.isEmpty();
    }

    @Override
    public String toString() {
        return "ReleaseUpstreamResponse{"
                + "finishedSchemaChangeEvents="
                + finishedSchemaChangeEvents
                + ", failedSchemaChangeEvents="
                + failedSchemaChangeEvents
                + ", ignoredSchemaChangeEvents="
                + ignoredSchemaChangeEvents
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ReleaseUpstreamResponse that = (ReleaseUpstreamResponse) object;
        return Objects.equals(finishedSchemaChangeEvents, that.finishedSchemaChangeEvents)
                && Objects.equals(failedSchemaChangeEvents, that.failedSchemaChangeEvents)
                && Objects.equals(ignoredSchemaChangeEvents, that.ignoredSchemaChangeEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                finishedSchemaChangeEvents, failedSchemaChangeEvents, ignoredSchemaChangeEvents);
    }
}
