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

package org.apache.flink.cdc.common.function;

import org.apache.flink.cdc.common.function.HashFunction.HashContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link HashContext}. */
class HashContextTest {

    @Test
    void testProperties() {
        HashContext context = HashFunction.createContext(3, 5);

        assertThat(context).isInstanceOf(DefaultHashContext.class);
        assertThat(context.getSourceSubtaskIndex()).isEqualTo(3);
        assertThat(context.getDownstreamParallelism()).isEqualTo(5);
    }

    @Test
    void testRejectsNegativeSourceSubtaskIndex() {
        assertThatThrownBy(() -> HashFunction.createContext(-1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sourceSubtaskIndex");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectsNonPositiveDownstreamParallelism(int downstreamParallelism) {
        assertThatThrownBy(() -> HashFunction.createContext(0, downstreamParallelism))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("downstreamParallelism");
    }
}
