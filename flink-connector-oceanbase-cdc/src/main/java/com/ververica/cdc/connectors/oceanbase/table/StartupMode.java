/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.table;

import org.apache.flink.table.api.ValidationException;

/** Startup modes for the OceanBase CDC Consumer. */
public enum StartupMode {
    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and
     * continue to read the commit log.
     */
    INITIAL,

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the end of the commit log which means only have the changes since the connector was started.
     */
    LATEST_OFFSET,

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read commit log from the specified timestamp.
     */
    TIMESTAMP;

    public static StartupMode getStartupMode(String modeString) {
        switch (modeString.toLowerCase()) {
            case "initial":
                return INITIAL;
            case "latest-offset":
                return LATEST_OFFSET;
            case "timestamp":
                return TIMESTAMP;
            default:
                throw new ValidationException(
                        String.format("Invalid startup mode '%s'.", modeString));
        }
    }
}
