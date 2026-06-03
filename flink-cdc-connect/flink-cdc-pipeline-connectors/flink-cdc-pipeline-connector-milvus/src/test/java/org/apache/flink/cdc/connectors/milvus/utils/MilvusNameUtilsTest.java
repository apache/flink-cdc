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

package org.apache.flink.cdc.connectors.milvus.utils;

import org.apache.flink.cdc.common.event.TableId;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Tests for {@link MilvusNameUtils}. */
class MilvusNameUtilsTest {

    @Test
    void testNormalizeTableIdToCollectionName() {
        Assertions.assertThat(
                        MilvusNameUtils.resolveCollectionName(
                                TableId.parse("inventory.public.orders"),
                                Collections.emptyMap(),
                                true))
                .isEqualTo("inventory_public_orders");
    }

    @Test
    void testPrefixCollectionNameStartingWithDigit() {
        Assertions.assertThat(MilvusNameUtils.normalizeIdentifier("2026.orders"))
                .isEqualTo("_2026_orders");
    }

    @Test
    void testRejectInvalidIdentifierWhenNormalizeDisabled() {
        Assertions.assertThatThrownBy(
                        () ->
                                MilvusNameUtils.resolveCollectionName(
                                        TableId.parse("inventory.orders"),
                                        Collections.emptyMap(),
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Milvus collection name");
    }

    @Test
    void testRejectInvalidVectorFieldName() {
        Assertions.assertThatThrownBy(() -> MilvusVectorFieldSpec.parse("bad-name:FloatVector(3)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Milvus vector field name");
    }
}
