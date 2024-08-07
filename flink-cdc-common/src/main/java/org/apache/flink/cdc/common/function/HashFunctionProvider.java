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

package org.apache.flink.cdc.common.function;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Provider that provides {@link HashFunction} to help {@code PrePartitionOperator} to shuffle event
 * to designated subtask. This is usually beneficial for load balancing, when writing to different
 * partitions/buckets in {@link DataSink}, add custom implementation to further improve efficiency.
 *
 * @param <T> the type of given element
 */
@Internal
public interface HashFunctionProvider<T> extends Serializable {

    /**
     * Gets a hash function based on the given table ID and schema, to help {@code
     * PrePartitionOperator} to shuffle {@link DataChangeEvent} to designated subtask.
     *
     * @param tableId table ID
     * @param schema flink table schema
     * @return hash function based on the given table ID and schema
     */
    HashFunction<T> getHashFunction(@Nullable TableId tableId, Schema schema);
}
