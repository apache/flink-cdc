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

package com.ververica.cdc.connectors.base.dialect;

import org.apache.flink.annotation.Experimental;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The dialect of data source.
 *
 * @param <C> The source config of data source.
 */
@Experimental
public interface DataSourceDialect<C extends SourceConfig> extends Serializable {

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

    /** Check if the CollectionId is case-sensitive or not. */
    boolean isDataCollectionIdCaseSensitive(C sourceConfig);

    /** Returns the {@link ChunkSplitter} which used to split collection to splits. */
    ChunkSplitter createChunkSplitter(C sourceConfig);

    /** The fetch task used to fetch data of a snapshot split or stream split. */
    FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase);

    /** The task context used for fetch task to fetch data from external systems. */
    FetchTask.Context createFetchTaskContext(SourceSplitBase sourceSplitBase, C sourceConfig);
}
