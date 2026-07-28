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

class LogicalFunctionsTest {

    @Test
    void testIfNull() {
        Assertions.assertThat(LogicalFunctions.ifNull(1, 2)).isEqualTo(1);
        Assertions.assertThat(LogicalFunctions.ifNull(null, 2)).isEqualTo(2);
        Assertions.assertThat((Object) LogicalFunctions.ifNull(null, null)).isNull();
    }

    @Test
    void testNullIf() {
        Assertions.assertThat(LogicalFunctions.nullIf(1, 1)).isNull();
        Assertions.assertThat(LogicalFunctions.nullIf(1, 2)).isEqualTo(1);
        Assertions.assertThat((Object) LogicalFunctions.nullIf(null, 1)).isNull();
        Assertions.assertThat(LogicalFunctions.nullIf(1, null)).isEqualTo(1);
        Assertions.assertThat(LogicalFunctions.nullIf(1, 1L)).isNull();
        Assertions.assertThat(LogicalFunctions.nullIf(new BigDecimal("1.00"), 1L)).isNull();
        Assertions.assertThat(LogicalFunctions.nullIf(1, 1.0d)).isNull();
        Assertions.assertThat(LogicalFunctions.nullIf(16_777_217, 16_777_216f)).isNull();
        Assertions.assertThat(LogicalFunctions.nullIf(new byte[] {1, 2}, new byte[] {1, 2}))
                .isNull();
    }
}
