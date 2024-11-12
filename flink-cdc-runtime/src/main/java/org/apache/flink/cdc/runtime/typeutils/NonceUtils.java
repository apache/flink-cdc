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

package org.apache.flink.cdc.runtime.typeutils;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.TableId;

import java.util.Objects;

/**
 * Generates schema evolution nonce value and corresponding {@link FlushEvent}s. It is guaranteed to
 * be unique by combining epoch timestamp, subTaskId, Table ID and schema change event into a long
 * hashCode.
 */
@PublicEvolving
public class NonceUtils {

    /**
     * Generating a nonce value with current @{code timestamp}, {@code subTaskId}, {@code tableId},
     * and {@code schemaChangeEvent}. The higher 32 bits are current UTC timestamp in epoch seconds,
     * and the lower 32 bits are Java hashCode of the rest parameters.
     */
    public static long generateNonce(
            int timestamp, int subTaskId, TableId tableId, Event schemaChangeEvent) {
        return (long) timestamp << 32
                | Integer.toUnsignedLong(Objects.hash(subTaskId, tableId, schemaChangeEvent));
    }

    /** Generating a {@link FlushEvent} carrying a nonce. */
    public static FlushEvent generateFlushEvent(
            int timestamp, int subTaskId, TableId tableId, Event schemaChangeEvent) {
        return new FlushEvent(
                tableId, generateNonce(timestamp, subTaskId, tableId, schemaChangeEvent));
    }
}
