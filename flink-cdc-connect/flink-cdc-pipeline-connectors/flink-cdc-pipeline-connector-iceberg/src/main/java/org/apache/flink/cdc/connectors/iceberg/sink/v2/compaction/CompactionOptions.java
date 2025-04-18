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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction;

import java.io.Serializable;

/** Strategy for small file compaction. */
public class CompactionOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    private int commitInterval = 1;
    private boolean enabled = false;
    private int parallelism = -1;

    // Private constructor to enforce the use of the Builder
    private CompactionOptions() {}

    public void setCommitInterval(int commitInterval) {
        this.commitInterval = commitInterval;
    }

    public int getCommitInterval() {
        return commitInterval;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link CompactionOptions}. */
    public static class Builder {

        private final CompactionOptions compactionOptions = new CompactionOptions();

        public Builder commitInterval(int commitInterval) {
            compactionOptions.setCommitInterval(commitInterval);
            return this;
        }

        public Builder enabled(boolean enabled) {
            compactionOptions.setEnabled(enabled);
            return this;
        }

        public Builder parallelism(int parallelism) {
            compactionOptions.setParallelism(parallelism);
            return this;
        }

        public CompactionOptions build() {
            return compactionOptions;
        }
    }
}
