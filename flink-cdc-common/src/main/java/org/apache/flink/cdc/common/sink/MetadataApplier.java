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

import java.io.Serializable;
import java.util.Set;

/** {@code MetadataApplier} is used to apply metadata changes to external systems. */
@PublicEvolving
public interface MetadataApplier extends Serializable {

    /** Checks if this metadata applier should handle this event type. */
    boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType);

    /** Checks what kind of schema change events downstream can handle. */
    Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes();

    /** Apply the given {@link SchemaChangeEvent} to external systems. */
    void applySchemaChange(SchemaChangeEvent schemaChangeEvent);
}
