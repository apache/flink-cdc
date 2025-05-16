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

package org.apache.flink.cdc.composer.flink.translator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test for {@link OperatorUidGenerator}. */
public class OperatorUidGeneratorTest {
    @ParameterizedTest
    @MethodSource
    public void testGenerateFromNonNullPrefix(String prefix, String suffix, String expected) {
        OperatorUidGenerator generator = new OperatorUidGenerator(prefix);
        assertThat(generator.generateUid(suffix)).isEqualTo(expected);
    }

    public static Stream<Arguments> testGenerateFromNonNullPrefix() {
        return Stream.of(
                Arguments.of("prefix1", "suffix1", "prefix1-suffix1"),
                Arguments.of("prefix1", "suffix2", "prefix1-suffix2"),
                Arguments.of("prefix2", "suffix1", "prefix2-suffix1"),
                Arguments.of("prefix2", "suffix2", "prefix2-suffix2"));
    }

    @Test
    public void testGenerateFromNullPrefix() {
        OperatorUidGenerator generator = new OperatorUidGenerator();
        assertThat(generator.generateUid("anything")).isNull();
    }

    @Test
    public void testEmptyPrefix() {
        assertThatThrownBy(() -> new OperatorUidGenerator(""))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }
}
