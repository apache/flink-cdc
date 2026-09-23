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

package org.apache.flink.cdc.common.pipeline;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

/**
 * Partitioning strategy for {@link org.apache.flink.cdc.common.event.DataChangeEvent} in Flink CDC
 * pipeline.
 *
 * <p>This enum defines different strategies used by {@code PrePartitionOperator} to determine which
 * downstream subtask an event should be routed to.
 */
@PublicEvolving
public enum HashFunctionStrategy {

    /**
     * Use the HashFunctionProvider defined by the sink.
     *
     * <p>This is the default behavior when {@code sink.partitioning.strategy} is not explicitly
     * set. The provider returned by the sink's {@code getDataChangeEventHashFunctionProvider}
     * method will be used, preserving backward compatibility with existing pipeline configurations.
     *
     * <p>This is the only supported strategy for sinks that require sink-defined routing, such as
     * paimon, fluss, and maxcompute.
     */
    SINK_DEFINED,

    /**
     * Hash by TableId and primary keys.
     *
     * <p>This strategy computes hash based on TableId (namespace, schema, table) combined with
     * primary key column values. Events with the same TableId and primary key will be routed to the
     * same subtask, while events from the same table with different primary keys may be spread
     * across multiple subtasks for load balancing.
     *
     * <p>This is suitable when you want good distribution of data across subtasks and don't require
     * strict ordering within a table.
     *
     * <p>This strategy is not supported for paimon, fluss, or maxcompute sinks.
     */
    PRIMARY_KEY,

    /**
     * Hash by TableId only.
     *
     * <p>This strategy computes hash based solely on TableId (namespace, schema, table), ignoring
     * the record payload entirely. All events from the same table will be routed to the same
     * subtask, ensuring per-table ordering semantics.
     *
     * <p>This is suitable when:
     *
     * <ul>
     *   <li>The sink requires strict single-writer-per-table semantics.
     *   <li>The table has no primary key or has a changing primary key set.
     *   <li>You want to avoid cross-subtask reordering within the same table.
     * </ul>
     *
     * <p>This strategy is not supported for paimon, fluss, or maxcompute sinks.
     */
    TABLE_ID
}
