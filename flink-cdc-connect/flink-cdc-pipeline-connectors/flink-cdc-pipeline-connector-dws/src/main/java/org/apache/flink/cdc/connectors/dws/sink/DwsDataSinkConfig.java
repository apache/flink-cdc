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

import org.apache.flink.cdc.common.utils.Preconditions;

import com.huaweicloud.dws.client.model.WriteMode;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;

/** Serializable, connector-owned configuration passed to DWS sink writers. */
public final class DwsDataSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long MEBIBYTE = 1024L * 1024L;

    private final String url;
    private final String username;
    private final String password;
    private final ZoneId zoneId;
    private final boolean caseSensitive;
    private final String defaultSchema;
    private final boolean enableDelete;
    private final boolean enableDnPartition;
    private final String distributionKey;
    private final WriteMode writeMode;
    private final boolean enableAutoFlush;
    private final int autoFlushBatchSize;
    private final Duration autoFlushMaxInterval;
    private final int writeThreadSize;
    private final int useCopyBatchSize;
    private final int forceFlushBatchSize;
    private final int retryMaxTimes;
    private final Duration retrySleepBaseTime;
    private final Duration retrySleepRandomTime;
    private final Duration taskTimeout;
    private final Duration statementTimeout;
    private final Duration connectionMaxUseTime;
    private final Duration connectionMaxIdle;
    private final long bufferAllMaxBytes;
    private final long bufferTableMaxBytes;
    private final long bufferPartitionMaxBytes;

    private DwsDataSinkConfig(Builder builder) {
        this.url = requireNonBlank(builder.url, "jdbc-url");
        this.username = requireNonBlank(builder.username, "username");
        this.password = Preconditions.checkNotNull(builder.password, "password must not be null");
        this.zoneId = Preconditions.checkNotNull(builder.zoneId, "zoneId must not be null");
        this.caseSensitive = builder.caseSensitive;
        this.defaultSchema = normalizeDefaultSchema(builder.defaultSchema);
        this.enableDelete = builder.enableDelete;
        this.enableDnPartition = builder.enableDnPartition;
        this.distributionKey = builder.distributionKey;
        this.writeMode = requireSupportedWriteMode(builder.writeMode);
        this.enableAutoFlush = builder.enableAutoFlush;
        this.autoFlushBatchSize =
                requirePositive(builder.autoFlushBatchSize, "auto flush batch size");
        this.autoFlushMaxInterval =
                requirePositive(builder.autoFlushMaxInterval, "auto flush interval");
        this.writeThreadSize = requirePositive(builder.writeThreadSize, "write thread size");
        this.useCopyBatchSize = requirePositive(builder.useCopyBatchSize, "COPY batch size");
        this.forceFlushBatchSize =
                requirePositive(builder.forceFlushBatchSize, "force flush batch size");
        Preconditions.checkArgument(
                !enableAutoFlush || forceFlushBatchSize >= autoFlushBatchSize,
                "Force flush batch size must be at least the auto flush batch size.");
        this.retryMaxTimes = requirePositive(builder.retryMaxTimes, "retry max times");
        this.retrySleepBaseTime = requireNonNegative(builder.retrySleepBaseTime, "retry base time");
        this.retrySleepRandomTime =
                requirePositive(builder.retrySleepRandomTime, "retry random time");
        this.taskTimeout = requirePositive(builder.taskTimeout, "task timeout");
        this.statementTimeout = requirePositive(builder.statementTimeout, "statement timeout");
        this.connectionMaxUseTime =
                requirePositive(builder.connectionMaxUseTime, "connection max use time");
        this.connectionMaxIdle =
                requirePositive(builder.connectionMaxIdle, "connection max idle time");
        this.bufferAllMaxBytes = requirePositive(builder.bufferAllMaxBytes, "all buffer bytes");
        this.bufferTableMaxBytes =
                requirePositive(builder.bufferTableMaxBytes, "table buffer bytes");
        this.bufferPartitionMaxBytes =
                requirePositive(builder.bufferPartitionMaxBytes, "partition buffer bytes");
        Preconditions.checkArgument(
                bufferPartitionMaxBytes <= bufferTableMaxBytes
                        && bufferTableMaxBytes <= bufferAllMaxBytes,
                "DWS buffer limits must satisfy partition <= table <= all.");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public boolean isEnableDelete() {
        return enableDelete;
    }

    public boolean isEnableDnPartition() {
        return enableDnPartition;
    }

    public String getDistributionKey() {
        return distributionKey;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public boolean isEnableAutoFlush() {
        return enableAutoFlush;
    }

    public int getAutoFlushBatchSize() {
        return autoFlushBatchSize;
    }

    public Duration getAutoFlushMaxInterval() {
        return autoFlushMaxInterval;
    }

    public int getWriteThreadSize() {
        return writeThreadSize;
    }

    public int getUseCopyBatchSize() {
        return useCopyBatchSize;
    }

    public int getForceFlushBatchSize() {
        return forceFlushBatchSize;
    }

    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }

    public Duration getRetrySleepBaseTime() {
        return retrySleepBaseTime;
    }

    public Duration getRetrySleepRandomTime() {
        return retrySleepRandomTime;
    }

    public Duration getTaskTimeout() {
        return taskTimeout;
    }

    public Duration getStatementTimeout() {
        return statementTimeout;
    }

    public Duration getConnectionMaxUseTime() {
        return connectionMaxUseTime;
    }

    public Duration getConnectionMaxIdle() {
        return connectionMaxIdle;
    }

    public long getBufferAllMaxBytes() {
        return bufferAllMaxBytes;
    }

    public long getBufferTableMaxBytes() {
        return bufferTableMaxBytes;
    }

    public long getBufferPartitionMaxBytes() {
        return bufferPartitionMaxBytes;
    }

    private static WriteMode requireSupportedWriteMode(WriteMode writeMode) {
        Preconditions.checkNotNull(writeMode, "write mode must not be null");
        Preconditions.checkArgument(
                writeMode == WriteMode.AUTO
                        || writeMode == WriteMode.UPSERT
                        || writeMode == WriteMode.COPY_UPSERT
                        || writeMode == WriteMode.COPY_MERGE,
                "Unsupported DWS CDC write mode: %s. Supported modes are AUTO, UPSERT, COPY_UPSERT, and COPY_MERGE.",
                writeMode);
        return writeMode;
    }

    private static String requireNonBlank(String value, String name) {
        Preconditions.checkNotNull(value, name + " must not be null");
        Preconditions.checkArgument(!value.trim().isEmpty(), name + " must not be blank");
        return value;
    }

    private static String normalizeDefaultSchema(String schema) {
        if (schema == null || schema.trim().isEmpty()) {
            return "public";
        }
        return schema.trim();
    }

    private static int requirePositive(int value, String name) {
        Preconditions.checkArgument(value > 0, name + " must be positive");
        return value;
    }

    private static long requirePositive(long value, String name) {
        Preconditions.checkArgument(value > 0, name + " must be positive");
        return value;
    }

    private static Duration requirePositive(Duration value, String name) {
        Preconditions.checkNotNull(value, name + " must not be null");
        Preconditions.checkArgument(
                !value.isZero() && !value.isNegative(), name + " must be positive");
        return value;
    }

    private static Duration requireNonNegative(Duration value, String name) {
        Preconditions.checkNotNull(value, name + " must not be null");
        Preconditions.checkArgument(!value.isNegative(), name + " must not be negative");
        return value;
    }

    /** Builder with connector-owned defaults, independent of client-library defaults. */
    public static final class Builder {
        private String url;
        private String username;
        private String password;
        private ZoneId zoneId = ZoneId.systemDefault();
        private boolean caseSensitive = true;
        private String defaultSchema = "public";
        private boolean enableDelete = true;
        private boolean enableDnPartition;
        private String distributionKey;
        private WriteMode writeMode = WriteMode.AUTO;
        private boolean enableAutoFlush = true;
        private int autoFlushBatchSize = 30_000;
        private Duration autoFlushMaxInterval = Duration.ofSeconds(3);
        private int writeThreadSize = 1;
        private int useCopyBatchSize = 1_000;
        private int forceFlushBatchSize = 40_000;
        private int retryMaxTimes = 3;
        private Duration retrySleepBaseTime = Duration.ofSeconds(1);
        private Duration retrySleepRandomTime = Duration.ofMillis(300);
        private Duration taskTimeout = Duration.ofMinutes(10);
        private Duration statementTimeout = Duration.ofMinutes(5);
        private Duration connectionMaxUseTime = Duration.ofHours(1);
        private Duration connectionMaxIdle = Duration.ofSeconds(60);
        private long bufferAllMaxBytes = 128L * MEBIBYTE;
        private long bufferTableMaxBytes = 64L * MEBIBYTE;
        private long bufferPartitionMaxBytes = 32L * MEBIBYTE;

        private Builder() {}

        public Builder withUrl(String value) {
            this.url = value;
            return this;
        }

        public Builder withUsername(String value) {
            this.username = value;
            return this;
        }

        public Builder withPassword(String value) {
            this.password = value;
            return this;
        }

        public Builder withZoneId(ZoneId value) {
            this.zoneId = value;
            return this;
        }

        public Builder withCaseSensitive(boolean value) {
            this.caseSensitive = value;
            return this;
        }

        public Builder withDefaultSchema(String value) {
            this.defaultSchema = value;
            return this;
        }

        public Builder withEnableDelete(boolean value) {
            this.enableDelete = value;
            return this;
        }

        public Builder withEnableDnPartition(boolean value) {
            this.enableDnPartition = value;
            return this;
        }

        public Builder withDistributionKey(String value) {
            this.distributionKey = value;
            return this;
        }

        public Builder withWriteMode(WriteMode value) {
            this.writeMode = value;
            return this;
        }

        public Builder withEnableAutoFlush(boolean value) {
            this.enableAutoFlush = value;
            return this;
        }

        public Builder withAutoFlushBatchSize(int value) {
            this.autoFlushBatchSize = value;
            return this;
        }

        public Builder withAutoFlushMaxInterval(Duration value) {
            this.autoFlushMaxInterval = value;
            return this;
        }

        public Builder withWriteThreadSize(int value) {
            this.writeThreadSize = value;
            return this;
        }

        public Builder withUseCopyBatchSize(int value) {
            this.useCopyBatchSize = value;
            return this;
        }

        public Builder withForceFlushBatchSize(int value) {
            this.forceFlushBatchSize = value;
            return this;
        }

        public Builder withRetryMaxTimes(int value) {
            this.retryMaxTimes = value;
            return this;
        }

        public Builder withRetrySleepBaseTime(Duration value) {
            this.retrySleepBaseTime = value;
            return this;
        }

        public Builder withRetrySleepRandomTime(Duration value) {
            this.retrySleepRandomTime = value;
            return this;
        }

        public Builder withTaskTimeout(Duration value) {
            this.taskTimeout = value;
            return this;
        }

        public Builder withStatementTimeout(Duration value) {
            this.statementTimeout = value;
            return this;
        }

        public Builder withConnectionMaxUseTime(Duration value) {
            this.connectionMaxUseTime = value;
            return this;
        }

        public Builder withConnectionMaxIdle(Duration value) {
            this.connectionMaxIdle = value;
            return this;
        }

        public Builder withBufferAllMaxBytes(long value) {
            this.bufferAllMaxBytes = value;
            return this;
        }

        public Builder withBufferTableMaxBytes(long value) {
            this.bufferTableMaxBytes = value;
            return this;
        }

        public Builder withBufferPartitionMaxBytes(long value) {
            this.bufferPartitionMaxBytes = value;
            return this;
        }

        public DwsDataSinkConfig build() {
            return new DwsDataSinkConfig(this);
        }
    }
}
