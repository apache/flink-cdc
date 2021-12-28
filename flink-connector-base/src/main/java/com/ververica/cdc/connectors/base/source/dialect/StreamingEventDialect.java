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

package com.ververica.cdc.connectors.base.source.dialect;

import com.ververica.cdc.connectors.base.source.split.StreamSplit;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.spi.SnapshotResult;

import java.io.Closeable;
import java.io.Serializable;

/** A dialect to handle database event during streaming process. */
public abstract class StreamingEventDialect implements Dialect, Closeable, Serializable {

    public abstract Task createTask(StreamSplit backfillStreamSplit);

    @Override
    public abstract void close();

    /** Task to read split of table. */
    public interface Task {

        SnapshotResult execute(ChangeEventSourceContext sourceContext);
    }

    /** Task to read stream split of table. */
    public class StreamSplitReadTask implements Task {
        @Override
        public SnapshotResult execute(ChangeEventSourceContext sourceContext) {
            return null;
        }
    }
}
