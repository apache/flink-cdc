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

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;

/**
 * A wrapper class that wraps {@link SourceReaderContext} for sharing message between {@link
 * MySqlSourceReader} and {@link MySqlSplitReader}.
 */
public class MySqlSourceReaderContext {

    private final SourceReaderContext sourceReaderContext;
    private volatile boolean isBinlogSplitReaderSuspended;
    private volatile boolean hasAssignedBinlogSplit;

    public MySqlSourceReaderContext(final SourceReaderContext sourceReaderContext) {
        this.sourceReaderContext = sourceReaderContext;
        this.isBinlogSplitReaderSuspended = false;
        this.hasAssignedBinlogSplit = false;
    }

    public SourceReaderContext getSourceReaderContext() {
        return sourceReaderContext;
    }

    public boolean isBinlogSplitReaderSuspended() {
        return isBinlogSplitReaderSuspended;
    }

    public void suspendBinlogSplitReader() {
        this.isBinlogSplitReaderSuspended = true;
    }

    public void wakeupSuspendedBinlogSplitReader() {
        this.isBinlogSplitReaderSuspended = false;
    }

    public boolean isHasAssignedBinlogSplit() {
        return hasAssignedBinlogSplit;
    }

    public void setHasAssignedBinlogSplit(boolean hasAssignedBinlogSplit) {
        this.hasAssignedBinlogSplit = hasAssignedBinlogSplit;
    }
}
