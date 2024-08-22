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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The response for {@link SchemaChangeRequest} from {@link SchemaRegistry} to {@link
 * SchemaOperator}.
 */
public class SchemaChangeResponse implements CoordinationResponse {
    private static final long serialVersionUID = 1L;

    /**
     * Whether the SchemaOperator need to buffer data and the SchemaOperatorCoordinator need to wait
     * for flushing.
     */
    private final List<SchemaChangeEvent> schemaChangeEvents;

    private final ResponseCode responseCode;

    public static SchemaChangeResponse accepted(List<SchemaChangeEvent> schemaChangeEvents) {
        return new SchemaChangeResponse(schemaChangeEvents, ResponseCode.ACCEPTED);
    }

    public static SchemaChangeResponse busy() {
        return new SchemaChangeResponse(Collections.emptyList(), ResponseCode.BUSY);
    }

    public static SchemaChangeResponse duplicate() {
        return new SchemaChangeResponse(Collections.emptyList(), ResponseCode.DUPLICATE);
    }

    public static SchemaChangeResponse ignored() {
        return new SchemaChangeResponse(Collections.emptyList(), ResponseCode.IGNORED);
    }

    private SchemaChangeResponse(
            List<SchemaChangeEvent> schemaChangeEvents, ResponseCode responseCode) {
        this.schemaChangeEvents = schemaChangeEvents;
        this.responseCode = responseCode;
    }

    public boolean isAccepted() {
        return ResponseCode.ACCEPTED.equals(responseCode);
    }

    public boolean isRegistryBusy() {
        return ResponseCode.BUSY.equals(responseCode);
    }

    public boolean isDuplicate() {
        return ResponseCode.DUPLICATE.equals(responseCode);
    }

    public boolean isIgnored() {
        return ResponseCode.IGNORED.equals(responseCode);
    }

    public List<SchemaChangeEvent> getSchemaChangeEvents() {
        return schemaChangeEvents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaChangeResponse)) {
            return false;
        }
        SchemaChangeResponse response = (SchemaChangeResponse) o;
        return Objects.equals(schemaChangeEvents, response.schemaChangeEvents)
                && responseCode == response.responseCode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaChangeEvents, responseCode);
    }

    @Override
    public String toString() {
        return "SchemaChangeResponse{"
                + "schemaChangeEvents="
                + schemaChangeEvents
                + ", responseCode="
                + responseCode
                + '}';
    }

    /**
     * Schema Change Response status code.
     *
     * <p>- Accepted: Requested schema change request has been accepted exclusively. Any other
     * schema change requests will be blocked.
     *
     * <p>- Busy: Schema registry is currently busy processing another schema change request.
     *
     * <p>- Duplicate: This schema change request has been submitted before, possibly by another
     * paralleled subTask.
     *
     * <p>- Ignored: This schema change request has been assessed, but no actual evolution is
     * required. Possibly caused by LENIENT mode or merging table strategies.
     */
    public enum ResponseCode {
        ACCEPTED,
        BUSY,
        DUPLICATE,
        IGNORED
    }
}
