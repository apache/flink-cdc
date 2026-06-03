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

package org.apache.flink.cdc.connectors.milvus.serde;

import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** Tests for {@link MilvusVectorConverter}. */
class MilvusVectorConverterTest {

    private static final MilvusVectorFieldSpec VECTOR_FIELD =
            MilvusVectorFieldSpec.parse("embedding:FloatVector(3)");

    @Test
    void testConvertListToFloatVector() {
        Assertions.assertThat(
                        MilvusVectorConverter.toFloatVector(
                                Arrays.asList(1, 2.5d, 3.25f), VECTOR_FIELD))
                .containsExactly(1.0f, 2.5f, 3.25f);
    }

    @Test
    void testConvertJsonArrayStringToFloatVector() {
        Assertions.assertThat(MilvusVectorConverter.toFloatVector("[1.0,2.5,3.25]", VECTOR_FIELD))
                .containsExactly(1.0f, 2.5f, 3.25f);
    }

    @Test
    void testRejectDimensionMismatch() {
        Assertions.assertThatThrownBy(
                        () ->
                                MilvusVectorConverter.toFloatVector(
                                        Arrays.asList(1.0f, 2.0f), VECTOR_FIELD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    void testRejectNonNumericVectorValue() {
        Assertions.assertThatThrownBy(
                        () ->
                                MilvusVectorConverter.toFloatVector(
                                        "[1.0,\"bad\",3.0]", VECTOR_FIELD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-numeric");
    }

    @Test
    void testRejectNonFiniteVectorValue() {
        Assertions.assertThatThrownBy(
                        () ->
                                MilvusVectorConverter.toFloatVector(
                                        Arrays.asList(1.0f, Float.NaN, 3.0f), VECTOR_FIELD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("finite");
    }
}
