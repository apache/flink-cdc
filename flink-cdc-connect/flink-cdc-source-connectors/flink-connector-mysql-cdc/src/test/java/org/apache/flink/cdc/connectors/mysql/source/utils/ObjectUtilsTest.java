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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

/** Tests for {@link org.apache.flink.cdc.connectors.mysql.source.utils.ObjectUtils}. */
class ObjectUtilsTest {

    @Test
    void testMinus() {
        Assertions.assertThat(ObjectUtils.minus(10000, 1)).isEqualTo(BigDecimal.valueOf(9999));
        Assertions.assertThat(ObjectUtils.minus(Integer.MAX_VALUE, Integer.MIN_VALUE))
                .isEqualTo(BigDecimal.valueOf(4294967295L));

        Assertions.assertThat(ObjectUtils.minus(10000000000000L, 1L))
                .isEqualTo(BigDecimal.valueOf(9999999999999L));

        Assertions.assertThat(ObjectUtils.minus(Long.MAX_VALUE, Long.MIN_VALUE))
                .isEqualTo(new BigDecimal("18446744073709551615"));

        Assertions.assertThat(
                        ObjectUtils.minus(new BigDecimal("100.12345"), new BigDecimal("1.00001")))
                .isEqualTo(new BigDecimal("99.12344"));
    }
}
