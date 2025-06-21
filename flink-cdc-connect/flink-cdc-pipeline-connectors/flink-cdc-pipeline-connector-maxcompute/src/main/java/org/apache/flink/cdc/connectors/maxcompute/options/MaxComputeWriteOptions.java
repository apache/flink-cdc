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

/** extended options for maxcompute. */
public class MaxComputeWriteOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int flushConcurrent;
    private final long maxBufferSize;
    private final long slotBufferSize;
    private final int numCommitThread;
    private final String compressAlgorithm;

    private MaxComputeWriteOptions(Builder builder) {
        this.flushConcurrent = builder.flushConcurrent;
        this.maxBufferSize = builder.maxBufferSize;
        this.slotBufferSize = builder.slotBufferSize;
        this.numCommitThread = builder.numCommitThread;
        this.compressAlgorithm = builder.compressAlgorithm.getValue();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getFlushConcurrent() {
        return flushConcurrent;
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public long getSlotBufferSize() {
        return slotBufferSize;
    }

    public int getNumCommitThread() {
        return numCommitThread;
    }

    public String getCompressAlgorithm() {
        return compressAlgorithm;
    }

    /** builder for maxcompute write options. */
    public static class Builder {
        private int flushConcurrent = 2;
        private long maxBufferSize = 64 * 1024 * 1024L;
        private long slotBufferSize = 1024 * 1024L;
        private int numCommitThread = 16;
        private CompressAlgorithm compressAlgorithm = CompressAlgorithm.ZLIB;

        public Builder withFlushConcurrent(int flushConcurrent) {
            this.flushConcurrent = flushConcurrent;
            return this;
        }

        public Builder withMaxBufferSize(long maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Builder withSlotBufferSize(long slotBufferSize) {
            this.slotBufferSize = slotBufferSize;
            return this;
        }

        public Builder withNumCommitThread(int numCommitThread) {
            this.numCommitThread = numCommitThread;
            return this;
        }

        public Builder withCompressAlgorithm(CompressAlgorithm compressAlgorithm) {
            this.compressAlgorithm = compressAlgorithm;
            return this;
        }

        public MaxComputeWriteOptions build() {
            return new MaxComputeWriteOptions(this);
        }
    }
}
