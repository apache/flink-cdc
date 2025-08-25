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

package org.apache.flink.cdc.connectors.base.dialect;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The dialect of data source.
 *
 * @param <C> The source config of data source.
 */
@Experimental
public interface DataSourceDialect<C extends SourceConfig> extends Serializable, Closeable {

    /** Get the name of dialect. */
    String getName();

    /** Discovers the list of data collection to capture. */
    List<TableId> discoverDataCollections(C sourceConfig);

    /**
     * Discovers the captured data collections' schema by {@link SourceConfig}.
     *
     * @param sourceConfig a basic source configuration.
     */
    Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(C sourceConfig);

    /**
     * Displays current offset from the database e.g. query Mysql binary logs by query <code>
     * SHOW MASTER STATUS</code>.
     */
    Offset displayCurrentOffset(C sourceConfig);

    /** Displays committed offset from the database e.g. query Postgresql confirmed_lsn */
    default Offset displayCommittedOffset(C sourceConfig) {
        throw new UnsupportedOperationException();
    }

    /** Check if the CollectionId is case-sensitive or not. */
    boolean isDataCollectionIdCaseSensitive(C sourceConfig);

    /** Returns the {@link ChunkSplitter} which used to split collection to splits. */
    @Deprecated
    ChunkSplitter createChunkSplitter(C sourceConfig);

    ChunkSplitter createChunkSplitter(C sourceConfig, ChunkSplitterState chunkSplitterState);

    /** The fetch task used to fetch data of a snapshot split or stream split. */
    FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase);

    /** The task context used for fetch task to fetch data from external systems. */
    FetchTask.Context createFetchTaskContext(C sourceConfig);

    /**
     * We may need the offset corresponding to the checkpointId. For example, we should commit LSN
     * of checkpoint to postgres's slot.
     */
    default void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {}

    /** Check if the tableId is included in SourceConfig. */
    boolean isIncludeDataCollection(C sourceConfig, TableId tableId);

    @Override
    default void close() throws IOException {}
}
