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

    /** The Elasticsearch index name to write to. */
    public static final ConfigOption<String> INDEX =
            ConfigOptions.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Elasticsearch index name to write to.");

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

    private ElasticsearchDataSinkOptions() {
        // This class should not be instantiated
    }
}
