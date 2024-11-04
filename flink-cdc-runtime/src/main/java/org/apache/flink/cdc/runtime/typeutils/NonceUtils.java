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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.TableId;

/** Generates schema evolution nonce value. */
public class NonceUtils {

    /** Calculates a hashCode with Long type instead of Integer. */
    public static long longHash(Object... a) {
        if (a == null) {
            return 0;
        }

        long result = 1;

        for (Object element : a) {
            result = 31L * result + (element == null ? 0 : element.hashCode());
        }

        return result;
    }

    public static long generateNonce(
            int versionCode, int subTaskId, TableId tableId, Event schemaChangeEvent) {
        return longHash(versionCode, subTaskId, tableId, schemaChangeEvent);
    }

    public static FlushEvent generateFlushEvent(
            int versionCode, int subTaskId, TableId tableId, Event schemaChangeEvent) {
        return new FlushEvent(
                tableId, generateNonce(versionCode, subTaskId, tableId, schemaChangeEvent));
    }
}
