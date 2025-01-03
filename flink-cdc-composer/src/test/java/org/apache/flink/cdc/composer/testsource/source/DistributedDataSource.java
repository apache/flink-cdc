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

package org.apache.flink.cdc.composer.testsource.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;

/** A {@link DataSource} that emits single-table-in-multi-partition data records. */
@Internal
public class DistributedDataSource implements DataSource {

    private final int tableCount;
    private final boolean distributedTables;

    public DistributedDataSource(int tableCount, boolean distributedTables) {
        this.tableCount = tableCount;
        this.distributedTables = distributedTables;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        return FlinkSourceFunctionProvider.of(
                new DistributedSourceFunction(tableCount, distributedTables));
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        throw new UnsupportedOperationException(
                "'Distributed' data source does not provide a metadata accessor.");
    }

    @Override
    public boolean isParallelMetadataSource() {
        // Since we're simulating a data source with distributed tables, metadata events might be
        // emitted in parallel.
        return distributedTables;
    }
}
