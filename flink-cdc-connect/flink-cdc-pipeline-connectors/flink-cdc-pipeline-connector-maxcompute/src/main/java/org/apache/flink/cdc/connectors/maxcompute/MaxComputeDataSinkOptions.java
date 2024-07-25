/*
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

package org.apache.flink.cdc.connectors.maxcompute;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

/** Options for MaxCompute Data Sink. */
public class MaxComputeDataSinkOptions {
    // basic options.
    public static final ConfigOption<String> ACCESS_ID =
            ConfigOptions.key("accessId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute user access id.");

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key("accessKey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute user access key.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute endpoint.");

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute project.");

    public static final ConfigOption<String> TUNNEL_ENDPOINT =
            ConfigOptions.key("tunnelEndpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute tunnel end point.");
    public static final ConfigOption<String> QUOTA_NAME =
            ConfigOptions.key("quotaName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "MaxCompute tunnel quota name, note that not quota nick-name.");

    public static final ConfigOption<String> STS_TOKEN =
            ConfigOptions.key("stsToken")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute sts token.");

    public static final ConfigOption<Integer> BUCKETS_NUM =
            ConfigOptions.key("bucketsNum")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "The batch size of MaxCompute table when automatically create table.");

    // write options.
    public static final ConfigOption<String> COMPRESS_ALGORITHM =
            ConfigOptions.key("compressAlgorithm")
                    .stringType()
                    .defaultValue("zlib")
                    .withDescription(
                            "The compress algorithm of data upload to MaxCompute, support 'zlib', 'snappy', 'raw'.");

    public static final ConfigOption<String> TOTAL_BATCH_SIZE =
            ConfigOptions.key("totalBatchSize")
                    .stringType()
                    .defaultValue("64MB")
                    .withDescription("The max batch size of data upload to MaxCompute.");

    public static final ConfigOption<String> BUCKET_BATCH_SIZE =
            ConfigOptions.key("bucketBatchSize")
                    .stringType()
                    .defaultValue("4MB")
                    .withDescription(
                            "The max batch size of data per bucket when upload to MaxCompute");

    public static final ConfigOption<Integer> NUM_COMMIT_THREADS =
            ConfigOptions.key("numCommitThreads")
                    .intType()
                    .defaultValue(16)
                    .withDescription("The number of threads used to commit data to MaxCompute.");

    public static final ConfigOption<Integer> NUM_FLUSH_CONCURRENT =
            ConfigOptions.key("numFlushConcurrent")
                    .intType()
                    .defaultValue(4)
                    .withDescription("The number of concurrent with flush bucket data.");
}
