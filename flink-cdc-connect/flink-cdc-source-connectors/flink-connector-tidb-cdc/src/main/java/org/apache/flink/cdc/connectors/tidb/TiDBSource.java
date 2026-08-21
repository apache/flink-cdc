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

package org.apache.flink.cdc.connectors.tidb;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.tidb.source.enumerator.TiKVEnumeratorState;
import org.apache.flink.cdc.connectors.tidb.source.enumerator.TiKVEnumeratorStateSerializer;
import org.apache.flink.cdc.connectors.tidb.source.enumerator.TiKVSourceEnumerator;
import org.apache.flink.cdc.connectors.tidb.source.reader.TiKVSourceReader;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplit;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplitSerializer;
import org.apache.flink.cdc.connectors.tidb.table.StartupMode;
import org.apache.flink.cdc.connectors.tidb.table.StartupOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.tikv.common.TiConfiguration;

/**
 * The TiDB CDC {@link Source} based on FLIP-27.
 *
 * <p>The enumerator splits the captured table by TiKV regions once and assigns a contiguous
 * key-range to each reader. Restore reuses checkpointed splits and does not re-query PD for region
 * topology.
 *
 * <pre>{@code
 * Source<String, ?, ?> source =
 *     TiDBSource.<String>builder()
 *         .database("mydb")
 *         .tableName("products")
 *         .tiConf(tiConf)
 *         .snapshotEventDeserializer(...)
 *         .changeEventDeserializer(...)
 *         .build();
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "TiDB Source");
 * }</pre>
 *
 * @param <T> the output type of the source.
 */
@PublicEvolving
public class TiDBSource<T>
        implements Source<T, TiKVKeyRangeSplit, TiKVEnumeratorState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private final TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
    private final TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
    private final TiConfiguration tiConf;
    private final StartupMode startupMode;
    private final String database;
    private final String tableName;

    TiDBSource(
            TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema,
            TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema,
            TiConfiguration tiConf,
            StartupMode startupMode,
            String database,
            String tableName) {
        this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
        this.changeEventDeserializationSchema = changeEventDeserializationSchema;
        this.tiConf = tiConf;
        this.startupMode = startupMode;
        this.database = database;
        this.tableName = tableName;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, TiKVKeyRangeSplit> createReader(SourceReaderContext readerContext) {
        return new TiKVSourceReader<>(
                readerContext,
                snapshotEventDeserializationSchema,
                changeEventDeserializationSchema,
                tiConf,
                startupMode);
    }

    @Override
    public SplitEnumerator<TiKVKeyRangeSplit, TiKVEnumeratorState> createEnumerator(
            SplitEnumeratorContext<TiKVKeyRangeSplit> enumContext) {
        return new TiKVSourceEnumerator(enumContext, tiConf, database, tableName);
    }

    @Override
    public SplitEnumerator<TiKVKeyRangeSplit, TiKVEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<TiKVKeyRangeSplit> enumContext, TiKVEnumeratorState checkpoint) {
        return new TiKVSourceEnumerator(enumContext, tiConf, database, tableName, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<TiKVKeyRangeSplit> getSplitSerializer() {
        return TiKVKeyRangeSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<TiKVEnumeratorState> getEnumeratorCheckpointSerializer() {
        return TiKVEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return snapshotEventDeserializationSchema.getProducedType();
    }

    /** Builder class of {@link TiDBSource}. */
    public static class Builder<T> {
        private String database;
        private String tableName;
        private StartupOptions startupOptions = StartupOptions.initial();
        private TiConfiguration tiConf;
        private TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
        private TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;

        /** Database name to be monitored. */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /** TableName name to be monitored. */
        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /** The deserializer used to convert from consumed snapshot event from TiKV. */
        public Builder<T> snapshotEventDeserializer(
                TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema) {
            this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
            return this;
        }

        /** The deserializer used to convert from consumed change event from TiKV. */
        public Builder<T> changeEventDeserializer(
                TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema) {
            this.changeEventDeserializationSchema = changeEventDeserializationSchema;
            return this;
        }

        /** Specifies the startup options. */
        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        /** TIDB config. */
        public Builder<T> tiConf(TiConfiguration tiConf) {
            this.tiConf = tiConf;
            return this;
        }

        public TiDBSource<T> build() {
            return new TiDBSource<>(
                    snapshotEventDeserializationSchema,
                    changeEventDeserializationSchema,
                    tiConf,
                    startupOptions.startupMode,
                    database,
                    tableName);
        }
    }
}
