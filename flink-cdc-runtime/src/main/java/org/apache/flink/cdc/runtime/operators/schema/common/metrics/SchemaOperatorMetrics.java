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

package org.apache.flink.cdc.runtime.operators.schema.common.metrics;

import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;

/** A collection class for handling metrics in {@link SchemaOperator}. */
public class SchemaOperatorMetrics {

    /** Current schema change behavior. */
    public static final String SCHEMA_CHANGE_BEHAVIOR = "schemaChangeBehavior";

    public static final Map<SchemaChangeBehavior, Integer> SCHEMA_CHANGE_BEHAVIOR_INTEGER_MAP =
            new HashMap<SchemaChangeBehavior, Integer>() {
                {
                    put(SchemaChangeBehavior.IGNORE, 0);
                    put(SchemaChangeBehavior.LENIENT, 1);
                    put(SchemaChangeBehavior.TRY_EVOLVE, 2);
                    put(SchemaChangeBehavior.EVOLVE, 3);
                    put(SchemaChangeBehavior.EXCEPTION, 4);
                }
            };

    /** Total count of schema change events received. */
    public static final String NUM_SCHEMA_CHANGE_EVENTS = "numSchemaChangeEvents";

    /** Number of successfully applied schema change events. */
    public static final String NUM_FINISHED_SCHEMA_CHANGE_EVENTS = "numFinishedSchemaChangeEvents";

    /** Number of schema change events that failed to apply. */
    public static final String NUM_FAILED_SCHEMA_CHANGE_EVENTS = "numFailedSchemaChangeEvents";

    /** Number of schema change events ignored. */
    public static final String NUM_IGNORED_SCHEMA_CHANGE_EVENTS = "numIgnoredSchemaChangeEvents";

    private final Counter numSchemaChangeEventsCounter;
    private final Counter numFinishedSchemaChangeEventsCounter;
    private final Counter numFailedSchemaChangeEventsCounter;
    private final Counter numIgnoredSchemaChangeEventsCounter;

    public SchemaOperatorMetrics(MetricGroup metricGroup, SchemaChangeBehavior behavior) {
        numSchemaChangeEventsCounter = metricGroup.counter(NUM_SCHEMA_CHANGE_EVENTS);
        numFinishedSchemaChangeEventsCounter =
                metricGroup.counter(NUM_FINISHED_SCHEMA_CHANGE_EVENTS);
        numFailedSchemaChangeEventsCounter = metricGroup.counter(NUM_FAILED_SCHEMA_CHANGE_EVENTS);
        numIgnoredSchemaChangeEventsCounter = metricGroup.counter(NUM_IGNORED_SCHEMA_CHANGE_EVENTS);
        metricGroup.gauge(
                SCHEMA_CHANGE_BEHAVIOR, () -> SCHEMA_CHANGE_BEHAVIOR_INTEGER_MAP.get(behavior));
    }

    public void increaseSchemaChangeEvents(long count) {
        numSchemaChangeEventsCounter.inc(count);
    }

    public void increaseFinishedSchemaChangeEvents(long count) {
        numFinishedSchemaChangeEventsCounter.inc(count);
    }

    public void increaseFailedSchemaChangeEvents(long count) {
        numFailedSchemaChangeEventsCounter.inc(count);
    }

    public void increaseIgnoredSchemaChangeEvents(long count) {
        numIgnoredSchemaChangeEventsCounter.inc(count);
    }
}
