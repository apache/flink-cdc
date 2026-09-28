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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DwsWriterState} and {@link DwsWriterStateSerializer}. */
class DwsWriterStateSerializerTest {

    @Test
    void testWriterStateRoundTrip() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();

        DwsWriterState restored =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(new DwsWriterState("job")));

        assertThat(restored.getJobId()).isEqualTo("job");
    }

    @Test
    void testRejectUnknownVersion() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();
        byte[] serialized = serializer.serialize(new DwsWriterState("job"));

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, serialized))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Unknown DWS writer state serializer version");
    }
}
