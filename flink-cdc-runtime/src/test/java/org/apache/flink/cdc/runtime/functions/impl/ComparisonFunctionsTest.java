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

package org.apache.flink.cdc.runtime.functions.impl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ComparisonFunctions}. */
class ComparisonFunctionsTest {

    @Test
    void testLegacyNullComparisonBehavior() {
        assertThat(ComparisonFunctions.valueEquals(null, 1)).isFalse();
        assertThat(ComparisonFunctions.valueEquals(1, null)).isFalse();
        assertThat(ComparisonFunctions.lessThan(null, 1)).isFalse();
        assertThat(ComparisonFunctions.lessThanOrEqual(1, null)).isFalse();
        assertThat(ComparisonFunctions.greaterThan(null, 1)).isFalse();
        assertThat(ComparisonFunctions.greaterThanOrEqual(1, null)).isFalse();
    }

    @Test
    void testThreeValuedLogicalOperators() {
        assertThat(LogicalFunctions.and(true, null)).isNull();
        assertThat(LogicalFunctions.and(false, null)).isFalse();
        assertThat(LogicalFunctions.or(false, null)).isNull();
        assertThat(LogicalFunctions.or(true, null)).isTrue();
        assertThat(LogicalFunctions.not(null)).isNull();
    }

    @Test
    void testLegacyBetweenNullBehavior() {
        assertThat(ComparisonFunctions.betweenAsymmetric((Integer) null, 1, 3)).isFalse();
        assertThat(ComparisonFunctions.notBetweenAsymmetric((Integer) null, 1, 3)).isTrue();
    }

    @Test
    void testLegacyInNullBehavior() {
        assertThat(ComparisonFunctions.in(1, 1, null)).isTrue();
        assertThat(ComparisonFunctions.in(1, 2, null)).isFalse();
        assertThat(ComparisonFunctions.in(null, 1, 2)).isFalse();
        assertThat(ComparisonFunctions.notIn(1, 2, null)).isTrue();
    }

    @Test
    void testDistinctFromNeverReturnsUnknown() {
        assertThat(ComparisonFunctions.isDistinctFrom(null, null)).isFalse();
        assertThat(ComparisonFunctions.isDistinctFrom(null, 1)).isTrue();
        assertThat(ComparisonFunctions.isDistinctFrom(1, 1)).isFalse();
        assertThat(ComparisonFunctions.isNotDistinctFrom(null, null)).isTrue();
        assertThat(ComparisonFunctions.isNotDistinctFrom(null, 1)).isFalse();
    }
}
