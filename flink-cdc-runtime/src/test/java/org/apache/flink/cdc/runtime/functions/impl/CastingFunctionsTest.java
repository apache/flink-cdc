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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.zone.ZoneRulesException;

class CastingFunctionsTest {

    @Test
    void testTryCastSupportedTypes() {
        Assertions.assertThat(CastingFunctions.tryCastToBoolean("true")).isTrue();
        Assertions.assertThat(CastingFunctions.tryCastToByte("1")).isEqualTo((byte) 1);
        Assertions.assertThat(CastingFunctions.tryCastToShort("2")).isEqualTo((short) 2);
        Assertions.assertThat(CastingFunctions.tryCastToInteger("3")).isEqualTo(3);
        Assertions.assertThat(CastingFunctions.tryCastToLong("4")).isEqualTo(4L);
        Assertions.assertThat(CastingFunctions.tryCastToFloat("5.5")).isEqualTo(5.5f);
        Assertions.assertThat(CastingFunctions.tryCastToDouble("6.5")).isEqualTo(6.5d);
        Assertions.assertThat(CastingFunctions.tryCastToBigDecimal("7.50", 3, 2))
                .isEqualByComparingTo(new BigDecimal("7.50"));
        Assertions.assertThat(CastingFunctions.tryCastToString(8)).isEqualTo("8");
        Assertions.assertThat(CastingFunctions.tryCastToTimestamp("2024-01-02T03:04:05", "UTC"))
                .isEqualTo(LocalDateTime.of(2024, 1, 2, 3, 4, 5));
        Assertions.assertThat(CastingFunctions.tryCastToBoolean("yes")).isTrue();
        Assertions.assertThat(CastingFunctions.tryCastToBoolean("no")).isFalse();
        Assertions.assertThat(CastingFunctions.tryCastToByte(128)).isEqualTo((byte) -128);
        Assertions.assertThat(CastingFunctions.tryCastToInteger(1.5d)).isEqualTo(1);
    }

    @Test
    void testTryCastInvalidDataReturnsNull() {
        Assertions.assertThat(CastingFunctions.tryCastToBoolean("invalid")).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToByte("128")).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToShort("32768")).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToInteger("1.5")).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToLong("9223372036854775808")).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToInteger("invalid")).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToDouble("invalid")).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToBigDecimal("invalid", 10, 2)).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToTimestamp("invalid-timestamp", "UTC"))
                .isNull();
        Assertions.assertThat(CastingFunctions.tryCastToInteger(null)).isNull();
        Assertions.assertThat(CastingFunctions.tryCastToTimestamp(null, "UTC")).isNull();
    }

    @Test
    void testTryCastDoesNotSuppressNonConversionErrors() {
        Assertions.assertThatThrownBy(
                        () ->
                                CastingFunctions.tryCastToTimestamp(
                                        "2024-01-02T03:04:05", "Invalid/Timezone"))
                .isExactlyInstanceOf(ZoneRulesException.class);
        Assertions.assertThatThrownBy(
                        () ->
                                CastingFunctions.tryCastToString(
                                        new Object() {
                                            @Override
                                            public String toString() {
                                                throw new IllegalStateException("internal error");
                                            }
                                        }))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("internal error");
    }

    @Test
    void testExistingCastFailureBehaviorIsUnchanged() {
        Assertions.assertThat(CastingFunctions.castToBoolean("invalid")).isFalse();
        Assertions.assertThat(CastingFunctions.castToByte("128")).isEqualTo((byte) -128);
        Assertions.assertThat(CastingFunctions.castToInteger("1.5")).isEqualTo(1);

        int[] toStringCalls = {0};
        Object invalidTimestamp =
                new Object() {
                    @Override
                    public String toString() {
                        toStringCalls[0]++;
                        return "invalid-timestamp";
                    }
                };
        Assertions.assertThatThrownBy(
                        () -> CastingFunctions.castToTimestamp(invalidTimestamp, "UTC"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unable to parse given string as timestamp: invalid-timestamp");
        Assertions.assertThat(toStringCalls[0]).isEqualTo(1);
    }
}
