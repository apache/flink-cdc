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

import java.math.BigDecimal;

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
    void testFlinkSqlNullComparisonBehavior() {
        assertThat(ComparisonFunctions.sqlValueEquals(null, 1)).isNull();
        assertThat(ComparisonFunctions.sqlNotEquals(1, null)).isNull();
        assertThat(ComparisonFunctions.sqlLessThan(null, 1)).isNull();
        assertThat(ComparisonFunctions.sqlLessThanOrEqual(1, null)).isNull();
        assertThat(ComparisonFunctions.sqlGreaterThan(null, 1)).isNull();
        assertThat(ComparisonFunctions.sqlGreaterThanOrEqual(1, null)).isNull();
        assertThat(ComparisonFunctions.sqlValueEquals(1, 1)).isTrue();
        assertThat(ComparisonFunctions.sqlGreaterThan(2, 1)).isTrue();
    }

    @Test
    void testDefaultNumericComparisonBehaviorRemainsUnchanged() {
        assertThat(ComparisonFunctions.valueEquals(1L, 1)).isFalse();
        assertThat(ComparisonFunctions.valueEquals(new BigDecimal("1.0"), new BigDecimal("1.00")))
                .isFalse();
        assertThat(
                        ComparisonFunctions.greaterThan(
                                9007199254740993L, new BigDecimal("9007199254740992")))
                .isFalse();
    }

    @Test
    void testFlinkSqlNumericComparisonBehavior() {
        assertThat(ComparisonFunctions.sqlValueEquals(1L, 1)).isTrue();
        assertThat(
                        ComparisonFunctions.sqlValueEquals(
                                new BigDecimal("1.0"), new BigDecimal("1.00")))
                .isTrue();
        assertThat(ComparisonFunctions.sqlNotEquals(1L, 2)).isTrue();
        assertThat(
                        ComparisonFunctions.sqlGreaterThan(
                                9007199254740993L, new BigDecimal("9007199254740992")))
                .isTrue();
        assertThat(
                        ComparisonFunctions.sqlLessThan(
                                new BigDecimal("9007199254740992"), 9007199254740993L))
                .isTrue();
        assertThat(ComparisonFunctions.sqlGreaterThanOrEqual(new BigDecimal("1.00"), 1L)).isTrue();
        assertThat(ComparisonFunctions.sqlLessThanOrEqual(1, new BigDecimal("1.0"))).isTrue();
    }

    @Test
    void testThreeValuedLogicalOperators() {
        assertThat(LogicalFunctions.and(true, (Boolean) null)).isNull();
        assertThat(LogicalFunctions.and(false, (Boolean) null)).isFalse();
        assertThat(LogicalFunctions.or(false, (Boolean) null)).isNull();
        assertThat(LogicalFunctions.or(true, (Boolean) null)).isTrue();
        assertThat(LogicalFunctions.not(null)).isNull();
    }

    @Test
    void testLogicalOperatorsShortCircuitRightOperand() {
        assertThat(LogicalFunctions.and(false, () -> failIfEvaluated())).isFalse();
        assertThat(LogicalFunctions.or(true, () -> failIfEvaluated())).isTrue();
    }

    @Test
    void testLegacyBetweenNullBehavior() {
        assertThat(ComparisonFunctions.betweenAsymmetric((Integer) null, 1, 3)).isFalse();
        assertThat(ComparisonFunctions.notBetweenAsymmetric((Integer) null, 1, 3)).isTrue();
    }

    @Test
    void testFlinkSqlBetweenNullBehavior() {
        assertThat(ComparisonFunctions.sqlBetween(null, 1, 3)).isNull();
        assertThat(ComparisonFunctions.sqlBetween(2, null, 3)).isNull();
        assertThat(ComparisonFunctions.sqlBetween(4, null, 3)).isFalse();
        assertThat(ComparisonFunctions.sqlNotBetween(null, 1, 3)).isNull();
    }

    @Test
    void testFlinkSqlBetweenUsesNumericComparison() {
        assertThat(
                        ComparisonFunctions.sqlBetween(
                                9007199254740993L,
                                BigDecimal.ZERO,
                                new BigDecimal("9007199254740992")))
                .isFalse();
        assertThat(
                        ComparisonFunctions.sqlBetween(
                                1L, new BigDecimal("1.00"), new BigDecimal("1.0")))
                .isTrue();
    }

    @Test
    void testLegacyInNullBehavior() {
        assertThat(ComparisonFunctions.in(1, 1, null)).isTrue();
        assertThat(ComparisonFunctions.in(1, 2, null)).isFalse();
        assertThat(ComparisonFunctions.in(null, 1, 2)).isFalse();
        assertThat(ComparisonFunctions.notIn(1, 2, null)).isTrue();
    }

    @Test
    void testFlinkSqlInNullBehavior() {
        assertThat(ComparisonFunctions.sqlIn(1, 1, null)).isTrue();
        assertThat(ComparisonFunctions.sqlIn(1, 2, null)).isNull();
        assertThat(ComparisonFunctions.sqlIn(null, 1, 2)).isNull();
        assertThat(ComparisonFunctions.sqlIn(1, 2, 3)).isFalse();
        assertThat(ComparisonFunctions.sqlNotIn(1, 2, null)).isNull();
    }

    @Test
    void testFlinkSqlInUsesNumericComparison() {
        assertThat(ComparisonFunctions.sqlIn(1L, 2, new BigDecimal("1.00"))).isTrue();
        assertThat(ComparisonFunctions.sqlNotIn(new BigDecimal("1.0"), 1L, null)).isFalse();
    }

    @Test
    void testDistinctFromNeverReturnsUnknown() {
        assertThat(ComparisonFunctions.isDistinctFrom(null, null)).isFalse();
        assertThat(ComparisonFunctions.isDistinctFrom(null, 1)).isTrue();
        assertThat(ComparisonFunctions.isDistinctFrom(1, 1)).isFalse();
        assertThat(ComparisonFunctions.isNotDistinctFrom(null, null)).isTrue();
        assertThat(ComparisonFunctions.isNotDistinctFrom(null, 1)).isFalse();
    }

    private static Boolean failIfEvaluated() {
        throw new AssertionError("Right operand should not be evaluated.");
    }
}
