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

package org.apache.flink.cdc.runtime.testutils.schema;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link MetadataApplier} for testing that holds all schema change events in a list for further
 * examination.
 */
public class CollectingMetadataApplier implements MetadataApplier {
    private final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>();

    private final Duration duration;
    private final Set<SchemaChangeEventType> enabledEventTypes;
    private final Set<SchemaChangeEventType> errorsOnEventTypes;

    public CollectingMetadataApplier(Duration duration) {
        this.duration = duration;
        this.enabledEventTypes =
                Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet());
        this.errorsOnEventTypes = Collections.emptySet();
    }

    public CollectingMetadataApplier(
            Duration duration, Set<SchemaChangeEventType> enabledEventTypes) {
        this.duration = duration;
        this.enabledEventTypes = enabledEventTypes;
        this.errorsOnEventTypes = Collections.emptySet();
    }

    public CollectingMetadataApplier(
            Duration duration,
            Set<SchemaChangeEventType> enabledEventTypes,
            Set<SchemaChangeEventType> errorsOnEventTypes) {
        this.duration = duration;
        this.enabledEventTypes = enabledEventTypes;
        this.errorsOnEventTypes = errorsOnEventTypes;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledEventTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet());
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        schemaChangeEvents.add(schemaChangeEvent);
        if (duration != null) {
            try {
                Thread.sleep(duration.toMillis());
                if (errorsOnEventTypes.contains(schemaChangeEvent.getType())) {
                    throw new UnsupportedSchemaChangeEventException(schemaChangeEvent);
                }
            } catch (InterruptedException ignore) {
                // Ignores sleep interruption
            }
        }
    }

    public List<SchemaChangeEvent> getSchemaChangeEvents() {
        return schemaChangeEvents;
    }
}
