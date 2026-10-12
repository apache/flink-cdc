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

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for checking end-of-split boundaries in Oracle LogMiner stream processing.
 *
 * <p>The original factory pattern (which produced {@code LogMinerEventProcessor} subclasses with an
 * overridden {@code processRow}) was removed in Debezium 3.4.2. The intercepted pre-processing now
 * happens via {@link LogMinerStreamingChangeEventSource#handleRowPreProcessing}, which calls {@link
 * #reachEndingOffset} to decide whether to stop at a split boundary.
 */
@Internal
public class EventProcessorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EventProcessorFactory.class);

    private EventProcessorFactory() {}

    /**
     * Checks whether the current LogMiner row has reached the ending offset of the given bounded
     * stream split. If so, dispatches the high-watermark event and signals the context to stop.
     *
     * @return {@code true} if the ending offset was reached (caller should skip further processing
     *     of this row), {@code false} otherwise
     */
    public static boolean reachEndingOffset(
            OraclePartition partition,
            LogMinerEventRow row,
            StreamSplit redoLogSplit,
            ErrorHandler errorHandler,
            WatermarkDispatcher dispatcher,
            ChangeEventSource.ChangeEventSourceContext context) {
        if (isBoundedRead(redoLogSplit)) {
            final RedoLogOffset currentRedoLogOffset = new RedoLogOffset(row.getScn().longValue());
            if (currentRedoLogOffset.isAtOrAfter(redoLogSplit.getEndingOffset())) {
                try {
                    dispatcher.dispatchWatermarkEvent(
                            partition.getSourcePartition(),
                            redoLogSplit,
                            currentRedoLogOffset,
                            WatermarkKind.END);
                } catch (InterruptedException e) {
                    LOG.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing redo log signal event", e));
                }
                ((StoppableChangeEventSourceContext) context).stopChangeEventSource();
                return true;
            }
        }
        return false;
    }

    private static boolean isBoundedRead(StreamSplit redoLogSplit) {
        return !RedoLogOffset.NO_STOPPING_OFFSET.equals(redoLogSplit.getEndingOffset());
    }
}
