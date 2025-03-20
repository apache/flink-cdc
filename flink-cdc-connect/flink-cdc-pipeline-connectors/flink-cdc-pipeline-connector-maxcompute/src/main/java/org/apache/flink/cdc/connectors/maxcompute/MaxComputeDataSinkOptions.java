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
import org.apache.flink.cdc.connectors.maxcompute.options.CompressAlgorithm;

/** Options for MaxCompute Data Sink. */
public class MaxComputeDataSinkOptions {
    // basic options.
    public static final ConfigOption<String> ACCESS_ID =
            ConfigOptions.key("access-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute user access id.");

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key("access-key")
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
            ConfigOptions.key("tunnel.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute tunnel end point.");

    public static final ConfigOption<String> QUOTA_NAME =
            ConfigOptions.key("quota.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "MaxCompute tunnel quota name, note that not quota nick-name.");

    public static final ConfigOption<String> STS_TOKEN =
            ConfigOptions.key("sts-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MaxCompute sts token.");

    public static final ConfigOption<Integer> BUCKETS_NUM =
            ConfigOptions.key("buckets-num")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "The batch size of MaxCompute table when automatically create table.");

    // write options.
    public static final ConfigOption<CompressAlgorithm> COMPRESS_ALGORITHM =
            ConfigOptions.key("compress.algorithm")
                    .enumType(CompressAlgorithm.class)
                    .defaultValue(CompressAlgorithm.ZLIB)
                    .withDescription(
                            "The compress algorithm of data upload to MaxCompute, support 'zlib', 'snappy', 'lz4', 'raw'.");

    public static final ConfigOption<String> BUCKET_BUFFER_SIZE =
            ConfigOptions.key("bucket.buffer-size")
                    .stringType()
                    .defaultValue("4MB")
                    .withDescription(
                            "The max batch size of data per bucket when upload to MaxCompute");

    public static final ConfigOption<String> TOTAL_BUFFER_SIZE =
            ConfigOptions.key("total.buffer-size")
                    .stringType()
                    .defaultValue("64MB")
                    .withDescription("The max batch size of data upload to MaxCompute.");

    public static final ConfigOption<Integer> COMMIT_THREAD_NUM =
            ConfigOptions.key("commit.thread-num")
                    .intType()
                    .defaultValue(16)
                    .withDescription("The number of threads used to commit data to MaxCompute.");

    public static final ConfigOption<Integer> FLUSH_CONCURRENT_NUM =
            ConfigOptions.key("flush.concurrent-num")
                    .intType()
                    .defaultValue(4)
                    .withDescription("The number of concurrent with flush bucket data.");
}
