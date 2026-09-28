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

package org.apache.flink.cdc.connectors.tidb.source.enumerator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Checkpoint state of {@link TiKVSourceEnumerator}.
 *
 * <p>{@code enumerated} must stay true after the first successful PD split so restore never
 * re-fetches region topology.
 */
@Internal
public class TiKVEnumeratorState {

    private final List<TiKVKeyRangeSplit> unassignedSplits;
    private final boolean enumerated;
    private final int parallelism;

    public TiKVEnumeratorState(List<TiKVKeyRangeSplit> unassignedSplits, boolean enumerated) {
        this(unassignedSplits, enumerated, -1);
    }

    public TiKVEnumeratorState(
            List<TiKVKeyRangeSplit> unassignedSplits, boolean enumerated, int parallelism) {
        this.unassignedSplits =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(unassignedSplits)));
        this.enumerated = enumerated;
        this.parallelism = parallelism;
    }

    public List<TiKVKeyRangeSplit> getUnassignedSplits() {
        return unassignedSplits;
    }

    public boolean isEnumerated() {
        return enumerated;
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TiKVEnumeratorState that = (TiKVEnumeratorState) o;
        return enumerated == that.enumerated
                && parallelism == that.parallelism
                && Objects.equals(unassignedSplits, that.unassignedSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unassignedSplits, enumerated, parallelism);
    }

    @Override
    public String toString() {
        return "TiKVEnumeratorState{enumerated="
                + enumerated
                + ", parallelism="
                + parallelism
                + ", unassigned="
                + unassignedSplits.size()
                + '}';
    }
}
