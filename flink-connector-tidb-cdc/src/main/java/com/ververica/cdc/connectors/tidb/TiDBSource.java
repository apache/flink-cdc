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

package com.ververica.cdc.connectors.tidb;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.tikv.common.TiConfiguration;

/** A builder to build a SourceFunction which can read snapshot and continue to read CDC events. */
public class TiDBSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
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

        public RichParallelSourceFunction<T> build() {

            return new TiKVRichParallelSourceFunction<>(
                    snapshotEventDeserializationSchema,
                    changeEventDeserializationSchema,
                    tiConf,
                    startupOptions.startupMode,
                    database,
                    tableName);
        }
    }
}
