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

package org.apache.flink.cdc.connectors.maxcompute;

import org.apache.flink.cdc.common.event.SchemaChangeEventType;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

class MaxComputeMetadataApplierTest {

    @Test
    void testAcceptedSchemaEvolutionTypes() {
        MaxComputeMetadataApplier applier = new MaxComputeMetadataApplier(null);

        assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.TRUNCATE_TABLE))
                .isTrue();
        assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.DROP_TABLE)).isTrue();

        applier.setAcceptedSchemaEvolutionTypes(
                EnumSet.complementOf(
                        EnumSet.of(
                                SchemaChangeEventType.TRUNCATE_TABLE,
                                SchemaChangeEventType.DROP_TABLE)));

        assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.TRUNCATE_TABLE))
                .isFalse();
        assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.DROP_TABLE)).isFalse();
        assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN)).isTrue();
    }
}
