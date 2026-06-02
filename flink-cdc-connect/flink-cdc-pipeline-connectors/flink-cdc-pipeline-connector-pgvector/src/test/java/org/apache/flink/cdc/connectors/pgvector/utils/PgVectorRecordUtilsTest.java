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

package org.apache.flink.cdc.connectors.pgvector.utils;

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link PgVectorRecordUtils}. */
class PgVectorRecordUtilsTest {

    @Test
    void testDenseVectorLiteral() {
        Assertions.assertThat(
                        PgVectorRecordUtils.toVectorLiteral(
                                Arrays.asList(1.0f, 2.5f, 3.0f),
                                new PgVectorColumnSpec(PgVectorType.VECTOR, 3)))
                .isEqualTo("[1.0,2.5,3.0]");
    }

    @Test
    void testHalfvecLiteral() {
        Assertions.assertThat(
                        PgVectorRecordUtils.toVectorLiteral(
                                "[0.5,1.5]", new PgVectorColumnSpec(PgVectorType.HALFVEC, 2)))
                .isEqualTo("[0.5,1.5]");
    }

    @Test
    void testSparseVectorLiteral() {
        Map<Object, Object> sparse = new HashMap<>();
        sparse.put(3, 0.25);
        sparse.put(1, 0.5);
        Assertions.assertThat(
                        PgVectorRecordUtils.toVectorLiteral(
                                sparse, new PgVectorColumnSpec(PgVectorType.SPARSEVEC, 4)))
                .isEqualTo("{1:0.5,3:0.25}/4");
    }

    @Test
    void testSparseVectorLiteralFromDenseInput() {
        Assertions.assertThat(
                        PgVectorRecordUtils.toVectorLiteral(
                                Arrays.asList(0.0f, 2.5f, 0.0f, 4.0f),
                                new PgVectorColumnSpec(PgVectorType.SPARSEVEC, 4)))
                .isEqualTo("{2:2.5,4:4.0}/4");
    }

    @Test
    void testBitVectorLiteralFromString() {
        Assertions.assertThat(
                        PgVectorRecordUtils.toVectorLiteral(
                                "1010", new PgVectorColumnSpec(PgVectorType.BIT, 4)))
                .isEqualTo("1010");
    }

    @Test
    void testBitVectorLiteralFromBytes() {
        Assertions.assertThat(
                        PgVectorRecordUtils.toVectorLiteral(
                                new byte[] {(byte) 0b10100000},
                                new PgVectorColumnSpec(PgVectorType.BIT, 8)))
                .isEqualTo("10100000");
    }

    @Test
    void testRejectDimensionMismatch() {
        Assertions.assertThatThrownBy(
                        () ->
                                PgVectorRecordUtils.toVectorLiteral(
                                        Arrays.asList(1.0f, 2.5f),
                                        new PgVectorColumnSpec(PgVectorType.VECTOR, 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    void testRejectDuplicateSparseIndex() {
        Map<Object, Object> sparse = new HashMap<>();
        sparse.put(1, 0.5);
        sparse.put("1", 0.25);
        Assertions.assertThatThrownBy(
                        () ->
                                PgVectorRecordUtils.toVectorLiteral(
                                        sparse, new PgVectorColumnSpec(PgVectorType.SPARSEVEC, 4)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicated");
    }

    @Test
    void testPrimaryKeysEqualHandlesByteArrayValues() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BYTES().notNull())
                        .physicalColumn("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        Assertions.assertThat(
                        PgVectorRecordUtils.primaryKeysEqual(
                                schema,
                                GenericRecordData.of(new byte[] {1, 2}, null),
                                GenericRecordData.of(new byte[] {1, 2}, null)))
                .isTrue();
        Assertions.assertThat(
                        PgVectorRecordUtils.primaryKeysEqual(
                                schema,
                                GenericRecordData.of(new byte[] {1, 2}, null),
                                GenericRecordData.of(new byte[] {2, 1}, null)))
                .isFalse();
    }
}
