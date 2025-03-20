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
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.maxcompute.options.CompressAlgorithm;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.configuration.MemorySize;

import java.util.HashSet;
import java.util.Set;

/** A {@link DataSinkFactory} for "MaxCompute" connector. */
public class MaxComputeDataSinkFactory implements DataSinkFactory {

    private static final String IDENTIFIER = "maxcompute";

    @Override
    public DataSink createDataSink(Context context) {
        MaxComputeOptions options =
                extractMaxComputeOptions(
                        context.getFactoryConfiguration(), context.getPipelineConfiguration());
        MaxComputeWriteOptions writeOptions =
                extractMaxComputeWriteOptions(context.getFactoryConfiguration());
        return new MaxComputeDataSink(options, writeOptions);
    }

    private MaxComputeOptions extractMaxComputeOptions(
            Configuration factoryConfiguration, Configuration pipelineConfiguration) {
        String accessId = factoryConfiguration.get(MaxComputeDataSinkOptions.ACCESS_ID);
        String accessKey = factoryConfiguration.get(MaxComputeDataSinkOptions.ACCESS_KEY);
        String endpoint = factoryConfiguration.get(MaxComputeDataSinkOptions.ENDPOINT);
        String project = factoryConfiguration.get(MaxComputeDataSinkOptions.PROJECT);
        String tunnelEndpoint = factoryConfiguration.get(MaxComputeDataSinkOptions.TUNNEL_ENDPOINT);
        String quotaName = factoryConfiguration.get(MaxComputeDataSinkOptions.QUOTA_NAME);
        String stsToken = factoryConfiguration.get(MaxComputeDataSinkOptions.STS_TOKEN);
        int bucketsNum = factoryConfiguration.get(MaxComputeDataSinkOptions.BUCKETS_NUM);

        String schemaOperatorUid =
                pipelineConfiguration.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID);
        return MaxComputeOptions.builder(accessId, accessKey, endpoint, project)
                .withTunnelEndpoint(tunnelEndpoint)
                .withQuotaName(quotaName)
                .withStsToken(stsToken)
                .withBucketsNum(bucketsNum)
                .withSchemaOperatorUid(schemaOperatorUid)
                .build();
    }

    private MaxComputeWriteOptions extractMaxComputeWriteOptions(
            Configuration factoryConfiguration) {
        int numCommitThread = factoryConfiguration.get(MaxComputeDataSinkOptions.COMMIT_THREAD_NUM);
        CompressAlgorithm compressAlgorithm =
                factoryConfiguration.get(MaxComputeDataSinkOptions.COMPRESS_ALGORITHM);
        int flushConcurrent =
                factoryConfiguration.get(MaxComputeDataSinkOptions.FLUSH_CONCURRENT_NUM);
        long maxBufferSize =
                MemorySize.parse(
                                factoryConfiguration.get(
                                        MaxComputeDataSinkOptions.TOTAL_BUFFER_SIZE))
                        .getBytes();
        long maxSlotSize =
                MemorySize.parse(
                                factoryConfiguration.get(
                                        MaxComputeDataSinkOptions.BUCKET_BUFFER_SIZE))
                        .getBytes();

        return MaxComputeWriteOptions.builder()
                .withNumCommitThread(numCommitThread)
                .withCompressAlgorithm(compressAlgorithm)
                .withFlushConcurrent(flushConcurrent)
                .withMaxBufferSize(maxBufferSize)
                .withSlotBufferSize(maxSlotSize)
                .build();
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(MaxComputeDataSinkOptions.ACCESS_ID);
        requiredOptions.add(MaxComputeDataSinkOptions.ACCESS_KEY);
        requiredOptions.add(MaxComputeDataSinkOptions.ENDPOINT);
        requiredOptions.add(MaxComputeDataSinkOptions.PROJECT);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        // options
        optionalOptions.add(MaxComputeDataSinkOptions.TUNNEL_ENDPOINT);
        optionalOptions.add(MaxComputeDataSinkOptions.QUOTA_NAME);
        optionalOptions.add(MaxComputeDataSinkOptions.STS_TOKEN);
        optionalOptions.add(MaxComputeDataSinkOptions.BUCKETS_NUM);
        // write options
        optionalOptions.add(MaxComputeDataSinkOptions.COMMIT_THREAD_NUM);
        optionalOptions.add(MaxComputeDataSinkOptions.COMPRESS_ALGORITHM);
        optionalOptions.add(MaxComputeDataSinkOptions.FLUSH_CONCURRENT_NUM);
        optionalOptions.add(MaxComputeDataSinkOptions.TOTAL_BUFFER_SIZE);
        optionalOptions.add(MaxComputeDataSinkOptions.BUCKET_BUFFER_SIZE);

        return optionalOptions;
    }
}
