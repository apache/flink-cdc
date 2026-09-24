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

package org.apache.flink.cdc.connectors.maxcompute.options;

import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;

import java.io.Serializable;

/** basic options for MaxCompute. */
public class MaxComputeOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String accessId;
    private final String accessKey;
    private final String endpoint;
    private final String project;
    private final String tunnelEndpoint;
    private final boolean supportSchema;
    private final String quotaName;
    private final String stsToken;
    private final int bucketsNum;
    private final String schemaOperatorUid;
    private final SinkOperation sinkOperation;

    private MaxComputeOptions(Builder builder) {
        this.accessId = builder.accessId;
        this.accessKey = builder.accessKey;
        this.endpoint = builder.endpoint;
        this.project = builder.project;
        this.tunnelEndpoint = builder.tunnelEndpoint;
        this.quotaName = builder.quotaName;
        this.stsToken = builder.stsToken;
        this.bucketsNum = builder.bucketsNum;
        this.supportSchema = MaxComputeUtils.supportSchema(this);
        this.schemaOperatorUid = builder.schemaOperatorUid;
        this.sinkOperation = builder.sinkOperation;
    }

    public static Builder builder(
            String accessId, String accessKey, String endpoint, String project) {
        return new Builder(accessId, accessKey, endpoint, project);
    }

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProject() {
        return project;
    }

    public String getQuotaName() {
        return quotaName;
    }

    public String getStsToken() {
        return stsToken;
    }

    public boolean isSupportSchema() {
        return supportSchema;
    }

    public int getBucketsNum() {
        return bucketsNum;
    }

    public String getSchemaOperatorUid() {
        return schemaOperatorUid;
    }

    public SinkOperation getSinkOperation() {
        return sinkOperation;
    }

    /** builder for maxcompute options. */
    public static class Builder {

        private final String accessId;
        private final String accessKey;
        private final String endpoint;
        private final String project;
        private String tunnelEndpoint;
        private String quotaName;
        private String stsToken;
        private String schemaOperatorUid;
        private int bucketsNum = 16;
        private SinkOperation sinkOperation = SinkOperation.UPSERT;

        public Builder(String accessId, String accessKey, String endpoint, String project) {
            this.accessId = accessId;
            this.accessKey = accessKey;
            this.endpoint = endpoint;
            this.project = project;
        }

        public Builder withTunnelEndpoint(String tunnelEndpoint) {
            this.tunnelEndpoint = tunnelEndpoint;
            return this;
        }

        public Builder withQuotaName(String quotaName) {
            this.quotaName = quotaName;
            return this;
        }

        public Builder withStsToken(String stsToken) {
            this.stsToken = stsToken;
            return this;
        }

        public Builder withBucketsNum(int bucketsNum) {
            this.bucketsNum = bucketsNum;
            return this;
        }

        public Builder withSchemaOperatorUid(String schemaOperatorUid) {
            this.schemaOperatorUid = schemaOperatorUid;
            return this;
        }

        public Builder withSinkOperation(SinkOperation sinkOperation) {
            this.sinkOperation = sinkOperation;
            return this;
        }

        public MaxComputeOptions build() {
            return new MaxComputeOptions(this);
        }
    }

    /** Sink operation mode for MaxCompute: APPEND or UPSERT. */
    public enum SinkOperation {
        APPEND("append"),
        UPSERT("upsert");

        private final String value;

        SinkOperation(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }

        public static SinkOperation fromValue(String value) {
            for (SinkOperation op : values()) {
                if (op.value.equalsIgnoreCase(value)) {
                    return op;
                }
            }
            throw new IllegalArgumentException(
                    "Unknown sink operation: '"
                            + value
                            + "'. Valid values are: 'upsert', 'append'.");
        }
    }
}
