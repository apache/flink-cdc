/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.base.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;

/**
 * A wrapper class that wraps {@link SourceReaderContext} for sharing message between {@link
 * IncrementalSourceReader} and {@link IncrementalSourceSplitReader}.
 */
public class IncrementalSourceReaderContext {

    private final SourceReaderContext sourceReaderContext;
    private volatile boolean stopStreamSplitReader;

    public IncrementalSourceReaderContext(final SourceReaderContext sourceReaderContext) {
        this.sourceReaderContext = sourceReaderContext;
        this.stopStreamSplitReader = false;
    }

    public SourceReaderContext getSourceReaderContext() {
        return sourceReaderContext;
    }

    public boolean needStopStreamSplitReader() {
        return stopStreamSplitReader;
    }

    public void setStopStreamSplitReader() {
        this.stopStreamSplitReader = true;
    }

    public void resetStopStreamSplitReader() {
        this.stopStreamSplitReader = false;
    }
}
