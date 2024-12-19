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

package org.apache.flink.cdc.runtime.operators.schema.regular.event;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaCoordinator;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The response for {@link SchemaChangeRequest} from {@link SchemaCoordinator} to {@link
 * SchemaOperator}.
 */
public class SchemaChangeResponse implements CoordinationResponse {
    private static final long serialVersionUID = 1L;

    /**
     * Actually finished schema change events. This will only be effective if status is {@code
     * accepted}.
     */
    private final List<SchemaChangeEvent> appliedSchemaChangeEvents;

    private final Map<TableId, Schema> evolvedSchemas;

    private final ResponseCode responseCode;

    public static SchemaChangeResponse success(
            List<SchemaChangeEvent> schemaChangeEvents, Map<TableId, Schema> evolvedSchemas) {
        return new SchemaChangeResponse(ResponseCode.SUCCESS, schemaChangeEvents, evolvedSchemas);
    }

    public static SchemaChangeResponse busy() {
        return new SchemaChangeResponse(ResponseCode.BUSY);
    }

    public static SchemaChangeResponse duplicate() {
        return new SchemaChangeResponse(ResponseCode.DUPLICATE);
    }

    public static SchemaChangeResponse ignored() {
        return new SchemaChangeResponse(ResponseCode.IGNORED);
    }

    public static SchemaChangeResponse waitingForFlush() {
        return new SchemaChangeResponse(ResponseCode.WAITING_FOR_FLUSH);
    }

    private SchemaChangeResponse(ResponseCode responseCode) {
        this(responseCode, Collections.emptyList(), Collections.emptyMap());
    }

    private SchemaChangeResponse(
            ResponseCode responseCode,
            List<SchemaChangeEvent> appliedSchemaChangeEvents,
            Map<TableId, Schema> evolvedSchemas) {
        this.responseCode = responseCode;
        this.appliedSchemaChangeEvents = appliedSchemaChangeEvents;
        this.evolvedSchemas = evolvedSchemas;
    }

    public boolean isSuccess() {
        return ResponseCode.SUCCESS.equals(responseCode);
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

    public boolean isWaitingForFlush() {
        return ResponseCode.WAITING_FOR_FLUSH.equals(responseCode);
    }

    public List<SchemaChangeEvent> getAppliedSchemaChangeEvents() {
        return appliedSchemaChangeEvents;
    }

    public Map<TableId, Schema> getEvolvedSchemas() {
        return evolvedSchemas;
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
        return Objects.equals(appliedSchemaChangeEvents, response.appliedSchemaChangeEvents)
                && responseCode == response.responseCode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(appliedSchemaChangeEvents, responseCode);
    }

    @Override
    public String toString() {
        return "SchemaChangeResponse{"
                + "schemaChangeEvents="
                + appliedSchemaChangeEvents
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
        SUCCESS,
        BUSY,
        DUPLICATE,
        IGNORED,
        WAITING_FOR_FLUSH
    }
}
