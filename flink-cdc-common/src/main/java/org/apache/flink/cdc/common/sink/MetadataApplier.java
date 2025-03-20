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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/** {@code MetadataApplier} is used to apply metadata changes to external systems. */
@PublicEvolving
public interface MetadataApplier extends Serializable, AutoCloseable {

    /** Apply the given {@link SchemaChangeEvent} to external systems. */
    void applySchemaChange(SchemaChangeEvent schemaChangeEvent) throws SchemaEvolveException;

    /** Sets enabled schema evolution event types of current metadata applier. */
    default MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        return this;
    }

    /** Checks if this metadata applier should this event type. */
    default boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return true;
    }

    /** Checks what kind of schema change events downstream can handle. */
    default Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet());
    }

    /** Closes the metadata applier and its underlying resources. */
    @Override
    default void close() throws Exception {}
}
