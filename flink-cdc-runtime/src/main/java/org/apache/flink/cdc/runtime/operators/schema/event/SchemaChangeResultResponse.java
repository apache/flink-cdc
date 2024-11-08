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

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.List;
import java.util.Objects;

/**
 * The response for {@link SchemaChangeResultRequest} from {@link SchemaRegistry} to {@link
 * SchemaOperator}.
 */
public class SchemaChangeResultResponse implements CoordinationResponse {

    private static final long serialVersionUID = 1L;

    /**
     * Whether the SchemaOperator need to buffer data and the SchemaOperatorCoordinator need to wait
     * for flushing.
     */
    private final List<SchemaChangeEvent> finishedSchemaChangeEvents;

    public SchemaChangeResultResponse(List<SchemaChangeEvent> finishedSchemaChangeEvents) {
        this.finishedSchemaChangeEvents = finishedSchemaChangeEvents;
    }

    public List<SchemaChangeEvent> getFinishedSchemaChangeEvents() {
        return finishedSchemaChangeEvents;
    }

    @Override
    public String toString() {
        return "ReleaseUpstreamResponse{"
                + "finishedSchemaChangeEvents="
                + finishedSchemaChangeEvents
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
        SchemaChangeResultResponse that = (SchemaChangeResultResponse) object;
        return Objects.equals(finishedSchemaChangeEvents, that.finishedSchemaChangeEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finishedSchemaChangeEvents);
    }
}
