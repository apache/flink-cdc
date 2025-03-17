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

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetContext;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EventSourceReader
        implements StreamingChangeEventSource<TiDBPartition, EventOffsetContext> {
    private static final Logger LOG = LoggerFactory.getLogger(EventSourceReader.class);

    public EventSourceReader(
            TiDBConnectorConfig dbzConnectorConfig,
            JdbcSourceEventDispatcher eventDispatcher,
            ErrorHandler errorHandler,
            TiDBSourceFetchTaskContext taskContext,
            StreamSplit split) {}

    @Override
    public void init() throws InterruptedException {
        StreamingChangeEventSource.super.init();
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            TiDBPartition partition,
            EventOffsetContext offsetContext)
            throws InterruptedException {}

    @Override
    public boolean executeIteration(
            ChangeEventSourceContext context,
            TiDBPartition partition,
            EventOffsetContext offsetContext)
            throws InterruptedException {
        return StreamingChangeEventSource.super.executeIteration(context, partition, offsetContext);
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        StreamingChangeEventSource.super.commitOffset(offset);
    }
}
