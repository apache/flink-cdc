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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for DWS SinkV2 serializers. */
class DwsCommittableSerializerTest {

    @Test
    void testCommittableRoundTrip() throws Exception {
        DwsCommittable committable =
                new DwsCommittable(
                        "job",
                        100L,
                        2,
                        "ods",
                        "orders",
                        "ods",
                        "flink_cdc_stage_abcd_100_2_0",
                        Arrays.asList("id", "name"),
                        Collections.singletonList("id"));
        DwsCommittableSerializer serializer = new DwsCommittableSerializer();

        DwsCommittable restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(restored.getJobId()).isEqualTo("job");
        assertThat(restored.getCheckpointId()).isEqualTo(100L);
        assertThat(restored.getSubtaskId()).isEqualTo(2);
        assertThat(restored.getTargetSchema()).isEqualTo("ods");
        assertThat(restored.getTargetTable()).isEqualTo("orders");
        assertThat(restored.getStagingSchema()).isEqualTo("ods");
        assertThat(restored.getStagingTable()).isEqualTo("flink_cdc_stage_abcd_100_2_0");
        assertThat(restored.getColumnNames()).containsExactly("id", "name");
        assertThat(restored.getPrimaryKeys()).containsExactly("id");
    }

    @Test
    void testWriterStateRoundTrip() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();

        DwsWriterState restored =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(new DwsWriterState("job")));

        assertThat(restored.getJobId()).isEqualTo("job");
    }
}
