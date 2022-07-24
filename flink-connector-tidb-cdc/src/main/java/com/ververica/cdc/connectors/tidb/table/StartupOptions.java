/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tidb.table;

import java.util.Objects;

/** TiDB CDC Source startup options. */
public final class StartupOptions {

    public final StartupMode startupMode;

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and
     * continue to read the latest CDC events.
     */
    public static StartupOptions initial() {
        return new StartupOptions(StartupMode.INITIAL);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the latest CDC events which means only have the changes since the connector was started.
     */
    public static StartupOptions latest() {
        return new StartupOptions(StartupMode.LATEST_OFFSET);
    }

    private StartupOptions(StartupMode startupMode) {
        this.startupMode = startupMode;

        switch (startupMode) {
            case INITIAL:

            case LATEST_OFFSET:
                break;

            default:
                throw new UnsupportedOperationException(startupMode + " mode is not supported.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupOptions that = (StartupOptions) o;
        return startupMode == that.startupMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startupMode);
    }
}
