/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.debezium.table;

import org.apache.flink.table.api.ValidationException;

/** Changelog modes used to encode changes from Debezium to Flink internal structure. */
public enum DebeziumChangelogMode {
    /** Default mode. It encodes changes as retract stream. */
    RETRACT,
    /** It encodes changes as upsert stream. */
    UPSERT;

    public static DebeziumChangelogMode getChangelogMode(String modeString) {
        switch (modeString.toLowerCase()) {
            case "retract":
                return RETRACT;
            case "upsert":
                return UPSERT;
            default:
                throw new ValidationException(
                        String.format("Invalid changelog mode '%s'.", modeString));
        }
    }
}
