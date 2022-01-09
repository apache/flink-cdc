package com.ververica.cdc.connectors.tidb; /*
                                            * Licensed to the Apache Software Foundation (ASF) under one
                                            * or more contributor license agreements.  See the NOTICE file
                                            * distributed with this work for additional information
                                            * regarding copyright ownership.  The ASF licenses this file
                                            * to you under the Apache License, Version 2.0 (the
                                            * "License"); you may not use this file except in compliance
                                            * with the License.  You may obtain a copy of the License at
                                            *
                                            *     http://www.apache.org/licenses/LICENSE-2.0
                                            *
                                            * Unless required by applicable law or agreed to in writing, software
                                            * distributed under the License is distributed on an "AS IS" BASIS,
                                            * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
                                            * See the License for the specific language governing permissions and
                                            * limitations under the License.
                                            */

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.tikv.common.TiConfiguration;
import org.tikv.common.meta.TiTableInfo;

import java.util.Map;

/** A builder to build a SourceFunction which can read snapshot and continue to read CDC events. */
public class TiDBSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link TiDBSource}. */
    public static class Builder<T> {
        private String hostname;
        private String database;
        private String[] tableList;
        private String username;
        private String password;
        private StartupOptions startupOptions = StartupOptions.initial();
        private Map<String, String> options;
        private TiConfiguration tiConf;
        private TiTableInfo tableInfo;

        private TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
        private TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /**
         * An optional list of regular expressions that match database names to be monitored; any
         * database name not included in the whitelist will be excluded from monitoring. By default
         * all databases will be monitored.
         */
        public Builder<T> database(String database) {
            this.database = database;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers for
         * tables to be monitored; any table not included in the list will be excluded from
         * monitoring. Each identifier is of the form schemaName.tableName. By default the connector
         * will monitor every non-system table in each monitored database.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        /** Name of the TiDB database to use when connecting to the TiDB database server. */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /** Password to use when connecting to the TiDB database server. */
        public Builder<T> password(String password) {
            this.password = password;
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

        public Builder<T> tiConf(TiConfiguration tiConf) {
            this.tiConf = tiConf;
            return this;
        }

        public Builder<T> tiTableInfo(TiTableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public RichParallelSourceFunction<T> build() {

            return new TiKVRichParallelSourceFunction<>(
                    snapshotEventDeserializationSchema,
                    changeEventDeserializationSchema,
                    tiConf,
                    tableInfo,
                    startupOptions.startupMode);
        }
    }
}
