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

import java.io.Serializable;

/** execution options for maxcompute. */
public class MaxComputeExecutionOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    /** builder for maxcompute execution options. */
    public static class Builder {
        private int maxRetries = 3;
        private long retryIntervalMillis = 3000;

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder withRetryIntervalMillis(long retryIntervalMillis) {
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }

        public MaxComputeExecutionOptions build() {
            return new MaxComputeExecutionOptions(this);
        }
    }

    private final int maxRetries;
    private final long retryIntervalMillis;

    private MaxComputeExecutionOptions(Builder builder) {
        this.maxRetries = builder.maxRetries;
        this.retryIntervalMillis = builder.retryIntervalMillis;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryIntervalMillis() {
        return retryIntervalMillis;
    }

    public static Builder builder() {
        return new Builder();
    }
}
