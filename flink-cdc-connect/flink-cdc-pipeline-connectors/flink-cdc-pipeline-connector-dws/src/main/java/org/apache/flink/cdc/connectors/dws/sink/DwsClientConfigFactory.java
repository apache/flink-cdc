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

package org.apache.flink.cdc.connectors.dws.sink;

import com.huaweicloud.dws.client.DwsConfig;
import com.huaweicloud.dws.client.model.Memory;
import com.huaweicloud.dws.client.model.PartitionPolicy;

import java.time.Duration;

import static com.huaweicloud.dws.client.config.DwsClientConfigs.ENABLE_COPY_COMPATIBLE_ILLEGAL_CHARS;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.JDBC_CONNECTION_MAX_IDLE;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.JDBC_CONNECTION_MAX_USE_TIME;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.JDBC_IS_DN;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.JDBC_PASSWORD;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.JDBC_URL;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.JDBC_USERNAME;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.RETRY_MAX_TIMES;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.RETRY_SLEEP_BASE_TIME;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.RETRY_SLEEP_RANDOM_TIME;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.TIMEOUT_SQL_STATEMENT;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.TIMEOUT_TASK;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_AUTO_FLUSH_BATCH_SIZE;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_AUTO_FLUSH_MAX_INTERVAL;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_BUFFER_ALL_MAX_BYTES;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_BUFFER_PARTITION_MAX_BYTES;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_BUFFER_TABLE_MAX_BYTES;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_FORCE_FLUSH_BATCH_SIZE;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_FORCE_FLUSH_UPSERT_BATCH_SIZE;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_FORMAT_STRING_U0000;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_MODE;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_PARTITION_MAX;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_PARTITION_MIN;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_PARTITION_POLICY;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_TABLE_FIELD_CASE_SENSITIVE;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_THREAD_SIZE;
import static com.huaweicloud.dws.client.config.DwsClientConfigs.WRITE_USE_COPY_BATCH_SIZE;

/** Builds the official-client configuration from the connector-owned immutable snapshot. */
public final class DwsClientConfigFactory {

    private DwsClientConfigFactory() {}

    public static DwsConfig create(DwsDataSinkConfig settings) {
        int nativeAutoFlushBatchSize =
                settings.isEnableAutoFlush()
                        ? settings.getAutoFlushBatchSize()
                        : settings.getForceFlushBatchSize();
        Duration nativeAutoFlushInterval =
                settings.isEnableAutoFlush() ? settings.getAutoFlushMaxInterval() : Duration.ZERO;

        return DwsConfig.of()
                .with(JDBC_URL, settings.getUrl())
                .with(JDBC_USERNAME, settings.getUsername())
                .with(JDBC_PASSWORD, settings.getPassword())
                .with(JDBC_IS_DN, false)
                .with(JDBC_CONNECTION_MAX_USE_TIME, settings.getConnectionMaxUseTime())
                .with(JDBC_CONNECTION_MAX_IDLE, settings.getConnectionMaxIdle())
                .with(WRITE_MODE, settings.getWriteMode())
                .with(WRITE_TABLE_FIELD_CASE_SENSITIVE, settings.isCaseSensitive())
                .with(WRITE_THREAD_SIZE, settings.getWriteThreadSize())
                .with(WRITE_USE_COPY_BATCH_SIZE, settings.getUseCopyBatchSize())
                .with(WRITE_AUTO_FLUSH_BATCH_SIZE, nativeAutoFlushBatchSize)
                .with(WRITE_AUTO_FLUSH_MAX_INTERVAL, nativeAutoFlushInterval)
                .with(WRITE_FORCE_FLUSH_BATCH_SIZE, settings.getForceFlushBatchSize())
                .with(WRITE_FORCE_FLUSH_UPSERT_BATCH_SIZE, settings.getForceFlushBatchSize())
                .with(WRITE_PARTITION_POLICY, PartitionPolicy.DYNAMIC)
                .with(WRITE_PARTITION_MIN, 1)
                .with(WRITE_PARTITION_MAX, 1)
                .with(WRITE_BUFFER_ALL_MAX_BYTES, new Memory(settings.getBufferAllMaxBytes()))
                .with(WRITE_BUFFER_TABLE_MAX_BYTES, new Memory(settings.getBufferTableMaxBytes()))
                .with(
                        WRITE_BUFFER_PARTITION_MAX_BYTES,
                        new Memory(settings.getBufferPartitionMaxBytes()))
                .with(RETRY_MAX_TIMES, settings.getRetryMaxTimes())
                .with(RETRY_SLEEP_BASE_TIME, settings.getRetrySleepBaseTime())
                .with(RETRY_SLEEP_RANDOM_TIME, settings.getRetrySleepRandomTime())
                .with(TIMEOUT_TASK, settings.getTaskTimeout())
                .with(TIMEOUT_SQL_STATEMENT, settings.getStatementTimeout())
                .with(WRITE_FORMAT_STRING_U0000, false)
                .with(ENABLE_COPY_COMPATIBLE_ILLEGAL_CHARS, false);
    }
}
