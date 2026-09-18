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

package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TiDBStreamFetchTask. */
public class TiDBStreamFetchTask implements FetchTask<SourceSplitBase> {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBStreamFetchTask.class);
    private final StreamSplit split;
    private volatile boolean taskRunning = false;
    private volatile boolean stopped = false;
    private volatile EventSourceReader eventSourceReader;
    private volatile StoppableChangeEventSourceContext changeEventSourceContext;

    public TiDBStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        if (stopped) {
            LOG.debug(
                    "StreamFetchTask for split: {} is already stopped and can not be executed",
                    split);
            return;
        } else {
            LOG.debug("execute StreamFetchTask for split: {}", split);
        }
        taskRunning = true;
        try {
            TiDBSourceFetchTaskContext sourceFetchContext = (TiDBSourceFetchTaskContext) context;
            sourceFetchContext.getOffsetContext().preSnapshotCompletion();

            EventSourceReader reader =
                    new EventSourceReader(
                            sourceFetchContext.getDbzConnectorConfig(),
                            sourceFetchContext.getEventDispatcher(),
                            sourceFetchContext.getErrorHandler(),
                            sourceFetchContext.getTaskContext(),
                            split);
            StoppableChangeEventSourceContext sourceContext =
                    new StoppableChangeEventSourceContext();
            this.eventSourceReader = reader;
            this.changeEventSourceContext = sourceContext;
            if (stopped) {
                reader.close();
                return;
            }
            reader.init();
            if (stopped) {
                reader.close();
                return;
            }
            reader.execute(
                    sourceContext,
                    sourceFetchContext.getPartition(),
                    sourceFetchContext.getOffsetContext());
        } finally {
            taskRunning = false;
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    @Override
    public void close() {
        LOG.debug("stopping StreamFetchTask for split: {}", split);
        stopped = true;
        taskRunning = false;
        StoppableChangeEventSourceContext sourceContext = changeEventSourceContext;
        if (sourceContext != null) {
            sourceContext.stopChangeEventSource();
        }
        EventSourceReader reader = eventSourceReader;
        if (reader != null) {
            reader.close();
        }
    }
}
