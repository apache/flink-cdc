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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.List;
import java.util.Map;

/** A mocked {@link StreamSplitAssigner} that could create multiple stream splits. */
public class MockedStreamSplitAssigner extends StreamSplitAssigner<MockedConfig> {

    public MockedStreamSplitAssigner(
            MockedConfig sourceConfig,
            DataSourceDialect<MockedConfig> dialect,
            OffsetFactory offsetFactory,
            SplitEnumeratorContext<? extends SourceSplit> enumeratorContext) {
        super(sourceConfig, dialect, offsetFactory, enumeratorContext);
    }

    @Override
    protected List<SourceSplitBase> createStreamSplits(
            MockedConfig sourceConfig,
            Offset minOffset,
            Offset stoppingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended,
            boolean isSnapshotCompleted) {
        return MockedDatabase.createStreamSplits(
                sourceConfig,
                minOffset,
                stoppingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                isSuspended,
                isSnapshotCompleted);
    }
}
