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

package org.apache.flink.cdc.connectors.base.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;

/**
 * A wrapper class that wraps {@link SourceReaderContext} for sharing message between {@link
 * IncrementalSourceReader} and {@link IncrementalSourceSplitReader}.
 */
public class IncrementalSourceReaderContext {
    private final SourceReaderContext sourceReaderContext;
    private volatile boolean isStreamSplitReaderSuspended;

    private volatile boolean hasAssignedStreamSplit;

    public IncrementalSourceReaderContext(SourceReaderContext sourceReaderContext) {
        this.sourceReaderContext = sourceReaderContext;
        this.isStreamSplitReaderSuspended = false;
        this.hasAssignedStreamSplit = false;
    }

    public SourceReaderContext getSourceReaderContext() {
        return sourceReaderContext;
    }

    public boolean isStreamSplitReaderSuspended() {
        return isStreamSplitReaderSuspended;
    }

    public void suspendStreamSplitReader() {
        this.isStreamSplitReaderSuspended = true;
    }

    public void wakeupSuspendedStreamSplitReader() {
        this.isStreamSplitReaderSuspended = false;
    }

    public boolean isHasAssignedStreamSplit() {
        return hasAssignedStreamSplit;
    }

    public void setHasAssignedStreamSplit(boolean hasAssignedStreamSplit) {
        this.hasAssignedStreamSplit = hasAssignedStreamSplit;
    }
}
