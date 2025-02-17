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

package org.apache.flink.cdc.connectors.elasticsearch.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

/** Options for the Elasticsearch data sink. */
public class ElasticsearchDataSinkOptions {

    /** The comma-separated list of Elasticsearch hosts to connect to. */
    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The comma-separated list of Elasticsearch hosts to connect to.");

    /** The maximum number of actions to buffer for each bulk request. */
    public static final ConfigOption<Integer> MAX_BATCH_SIZE =
            ConfigOptions.key("batch.size.max")
                    .intType()
                    .defaultValue(500)
                    .withDescription(
                            "The maximum number of actions to buffer for each bulk request.");

    /** The maximum number of concurrent requests that the sink will try to execute. */
    public static final ConfigOption<Integer> MAX_IN_FLIGHT_REQUESTS =
            ConfigOptions.key("inflight.requests.max")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The maximum number of concurrent requests that the sink will try to execute.");

    /** The maximum number of requests to keep in the in-memory buffer. */
    public static final ConfigOption<Integer> MAX_BUFFERED_REQUESTS =
            ConfigOptions.key("buffered.requests.max")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The maximum number of requests to keep in the in-memory buffer.");

    /** The maximum size of batch requests in bytes. */
    public static final ConfigOption<Long> MAX_BATCH_SIZE_IN_BYTES =
            ConfigOptions.key("batch.size.max.bytes")
                    .longType()
                    .defaultValue(5L * 1024L * 1024L)
                    .withDescription("The maximum size of batch requests in bytes.");

    /** The maximum time to wait for incomplete batches before flushing. */
    public static final ConfigOption<Long> MAX_TIME_IN_BUFFER_MS =
            ConfigOptions.key("buffer.time.max.ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription(
                            "The maximum time to wait for incomplete batches before flushing.");

    /** The maximum size of a single record in bytes. */
    public static final ConfigOption<Long> MAX_RECORD_SIZE_IN_BYTES =
            ConfigOptions.key("record.size.max.bytes")
                    .longType()
                    .defaultValue(10L * 1024L * 1024L)
                    .withDescription("The maximum size of a single record in bytes.");

    /** The version of Elasticsearch to connect to. */
    public static final ConfigOption<Integer> VERSION =
            ConfigOptions.key("version")
                    .intType()
                    .defaultValue(7)
                    .withDescription("The version of Elasticsearch to connect to (6, 7, or 8).");

    /** The username for Elasticsearch authentication. */
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for Elasticsearch authentication.");

    /** The password for Elasticsearch authentication. */
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for Elasticsearch authentication.");

    /** The sharding for Elasticsearch index, default sink table name is test_table_${suffix}. */
    public static final ConfigOption<String> SHARDING_SUFFIX_KEY =
            ConfigOptions.key("sharding.suffix.key")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Sharding suffix key for each table, allow setting sharding suffix key for multiTables.Default sink table name is test_table${suffix_key}.Default sharding column is first partition column.Tables are separated by ';'.Table and column are separated by '$'.For example, we can set sharding.suffix.key by 'table1$col1;table2$col2'");

    /** The sharding for Elasticsearch index, default sink table name is test_table_${suffix}. */
    public static final ConfigOption<String> SHARDING_SUFFIX_SEPARATOR =
            ConfigOptions.key("sharding.suffix.separator")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Separator for sharding suffix in table names, allow defining the separator between table name and sharding suffix. Default value is '_'. For example, if set to '-', the default table name would be test_table-${suffix}");

    private ElasticsearchDataSinkOptions() {
        // This class should not be instantiated
    }
}
