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

package com.ververica.cdc.connectors.postgres.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.enumerator.SnapshotSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.postgres.source.PostgresDialect;

/**
 * The Postgres snapshot enumerator that enumerates receive the split request and assign the split
 * to source readers.
 */
public class PostgresSnapshotSourceEnumerator extends SnapshotSourceEnumerator
        implements PostgresEnumerator {

    private final PostgresDialect postgresDialect;

    public PostgresSnapshotSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            SplitAssigner splitAssigner,
            PostgresDialect postgresDialect) {
        super(context, splitAssigner);
        this.postgresDialect = postgresDialect;
    }

    @Override
    public void start() {
        createSlotForGlobalStreamSplit(postgresDialect);
        super.start();
    }
}
