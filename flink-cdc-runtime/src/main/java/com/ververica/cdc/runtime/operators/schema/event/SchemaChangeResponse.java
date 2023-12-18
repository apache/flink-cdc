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

package com.ververica.cdc.runtime.operators.schema.event;

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import com.ververica.cdc.runtime.operators.schema.SchemaOperator;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;

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
    private final boolean shouldSendFlushEvent;

    public SchemaChangeResponse(boolean shouldSendFlushEvent) {
        this.shouldSendFlushEvent = shouldSendFlushEvent;
    }

    public boolean isShouldSendFlushEvent() {
        return shouldSendFlushEvent;
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
        return shouldSendFlushEvent == response.shouldSendFlushEvent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shouldSendFlushEvent);
    }
}
