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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.schema.Schema;

/** A {@link SchemaChangeEvent} that supports appending schema before change event. */
@PublicEvolving
public interface SchemaChangeEventWithPreSchema extends SchemaChangeEvent {

    /** Describes if this event already has schema before change info. */
    boolean hasPreSchema();

    /** Append schema before change info to this event. */
    void fillPreSchema(Schema oldSchema);

    /** Check if this event contains redundant schema change request only. */
    default boolean trimRedundantChanges() {
        return false;
    }
}
