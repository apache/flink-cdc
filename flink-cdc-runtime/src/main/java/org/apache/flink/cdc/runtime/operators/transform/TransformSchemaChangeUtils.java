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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.slf4j.Logger;

import java.util.Optional;

/**
 * Shared utility methods for filtering schema change events in both {@link PreTransformOperator}
 * and {@link PostTransformOperator}.
 */
class TransformSchemaChangeUtils {

    /**
     * Filters duplicate {@link AddColumnEvent} columns that already exist in the given schema. For
     * non-AddColumnEvent schema changes, the event is returned as-is.
     *
     * @param currentSchema the current schema to check against
     * @param event the schema change event to filter
     * @param log the logger to use for debug messages
     * @return the filtered event, or {@link Optional#empty()} if the event is fully redundant
     */
    static Optional<SchemaChangeEvent> filterDuplicateAddColumns(
            Schema currentSchema, SchemaChangeEvent event, Logger log) {
        if (!(event instanceof AddColumnEvent)) {
            return Optional.of(event);
        }
        Optional<AddColumnEvent> filtered =
                SchemaUtils.filterRedundantAddColumns(currentSchema, (AddColumnEvent) event);
        if (!filtered.isPresent()) {
            log.debug(
                    "Skipping fully redundant AddColumnEvent for table {} "
                            + "- all columns already exist",
                    event.tableId());
            return Optional.empty();
        }
        return Optional.of(filtered.get());
    }
}
