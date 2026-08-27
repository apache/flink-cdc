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

package org.apache.flink.cdc.connectors.fluss.source.enumerator;

import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlussSourceEnumStateSerializer}. */
class FlussSourceEnumStateSerializerTest {

    @Test
    void testSerializeLeaseIdInVersionOneState() throws Exception {
        PhysicalTablePath physicalTablePath =
                PhysicalTablePath.of(TablePath.of("database", "table"));
        FlussSplitBase split = new FlussLogSplit(physicalTablePath, new TableBucket(1L, 0), 42L);
        FlussSourceEnumState state =
                new FlussSourceEnumState(
                        Collections.singleton(physicalTablePath),
                        Collections.singletonList(split),
                        "lease-id");
        FlussSourceEnumStateSerializer serializer = new FlussSourceEnumStateSerializer();

        FlussSourceEnumState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(serializer.getVersion()).isEqualTo(1);
        assertThat(restored.getAssignedPhysicalTablePaths()).containsExactly(physicalTablePath);
        assertThat(restored.getRemainingSplits()).containsExactly(split);
        assertThat(restored.getLeaseId()).isEqualTo("lease-id");
    }
}
