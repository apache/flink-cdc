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

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;

import java.util.Objects;

/** A {@link OperatorEvent} that register sink writer to {@link SchemaRegistry}. */
public class SinkWriterRegisterEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final int subtask;

    public SinkWriterRegisterEvent(int subtask) {
        this.subtask = subtask;
    }

    public int getSubtask() {
        return subtask;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SinkWriterRegisterEvent)) {
            return false;
        }
        SinkWriterRegisterEvent that = (SinkWriterRegisterEvent) o;
        return subtask == that.subtask;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtask);
    }
}
