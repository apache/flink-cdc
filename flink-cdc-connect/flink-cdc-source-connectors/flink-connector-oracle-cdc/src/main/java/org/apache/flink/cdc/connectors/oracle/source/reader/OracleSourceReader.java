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

package org.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;

/**
 * Oracle source reader that only externalizes stream offsets after the stream split is safe to
 * commit.
 *
 * <p>When newly added table scanning is enabled, the stream split can be suspended and later
 * recreated with a reset start offset after new snapshot metadata is collected. Offsets observed
 * before that reset must not be committed to Oracle XStream processed low watermark.
 */
@Experimental
public class OracleSourceReader extends IncrementalSourceReaderWithCommit {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSourceReader.class);

    private volatile boolean isCommitOffset;

    public OracleSourceReader(
            FutureCompletingBlockingQueue elementQueue,
            Supplier supplier,
            RecordEmitter recordEmitter,
            Configuration config,
            IncrementalSourceReaderContext incrementalSourceReaderContext,
            SourceConfig sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect dialect) {
        super(
                elementQueue,
                supplier,
                recordEmitter,
                config,
                incrementalSourceReaderContext,
                sourceConfig,
                sourceSplitSerializer,
                dialect);
        this.isCommitOffset = false;
    }

    @Override
    protected void updateStreamSplitFinishedSplitsSize(
            LatestFinishedSplitsNumberEvent sourceEvent) {
        super.updateStreamSplitFinishedSplitsSize(sourceEvent);
        isCommitOffset = true;
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        List<SourceSplitBase> sourceSplitBases = super.snapshotState(checkpointId);
        if (!isCommitOffset()) {
            LOG.debug("Close Oracle XStream offset commit for checkpoint {}", checkpointId);
            lastCheckpointOffsets.remove(checkpointId);
        }
        return sourceSplitBases;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (isCommitOffset()) {
            super.notifyCheckpointComplete(checkpointId);
        } else {
            LOG.debug("Skip Oracle XStream offset commit for checkpoint {}", checkpointId);
        }
    }

    private boolean isCommitOffset() {
        return !sourceConfig.isScanNewlyAddedTableEnabled() || isCommitOffset;
    }
}
