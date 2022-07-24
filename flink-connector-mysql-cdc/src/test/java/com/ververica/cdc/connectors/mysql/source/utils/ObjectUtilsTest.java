/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.utils;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

/** Tests for {@link ObjectUtils}. */
public class ObjectUtilsTest {

    @Test
    public void testMinus() {
        assertEquals(BigDecimal.valueOf(9999), ObjectUtils.minus(10000, 1));
        assertEquals(
                BigDecimal.valueOf(4294967295L),
                ObjectUtils.minus(Integer.MAX_VALUE, Integer.MIN_VALUE));

        assertEquals(BigDecimal.valueOf(9999999999999L), ObjectUtils.minus(10000000000000L, 1L));
        assertEquals(
                new BigDecimal("18446744073709551615"),
                ObjectUtils.minus(Long.MAX_VALUE, Long.MIN_VALUE));

        assertEquals(
                new BigDecimal("99.12344"),
                ObjectUtils.minus(new BigDecimal("100.12345"), new BigDecimal("1.00001")));
    }
}
