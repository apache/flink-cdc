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

/** Route mode for routing rules. */
@PublicEvolving
public enum RouteMode {
    /** Match all applicable routing rules. */
    ALL_MATCH("all-match"),
    /** Match only the first applicable routing rule. */
    FIRST_MATCH("first-match");

    private final String value;

    RouteMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    /**
     * Parse RouteMode from string. Supports both underscore (ALL_MATCH) and hyphen (all-match)
     * formats, case-insensitive.
     *
     * @param value the string value to parse
     * @return the corresponding RouteMode
     * @throws IllegalArgumentException if the value cannot be parsed
     */
    public static RouteMode fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("RouteMode value cannot be null");
        }
        for (RouteMode mode : RouteMode.values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "Invalid RouteMode value: '%s'. Expected one of: [all-match, first-match, ALL-MATCH, FIRST-MATCH]",
                        value));
    }
}
