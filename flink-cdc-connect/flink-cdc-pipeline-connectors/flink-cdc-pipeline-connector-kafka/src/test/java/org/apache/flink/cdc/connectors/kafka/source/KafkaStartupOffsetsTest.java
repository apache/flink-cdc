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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** Tests for {@link KafkaStartupOffsets}. */
class KafkaStartupOffsetsTest {

    @Test
    void testParseSingleTopicEntries() {
        Map<TopicPartition, Long> offsets =
                KafkaStartupOffsets.parse("partition:0,offset:42;partition:1,offset:300", "orders");

        Assertions.assertThat(offsets)
                .containsEntry(new TopicPartition("orders", 0), 42L)
                .containsEntry(new TopicPartition("orders", 1), 300L)
                .hasSize(2);
    }

    @Test
    void testParseMultiTopicEntries() {
        Map<TopicPartition, Long> offsets =
                KafkaStartupOffsets.parse(
                        "topic:dbz.customers,partition:0,offset:42;topic:dbz.orders,partition:1,offset:10",
                        null);

        Assertions.assertThat(offsets)
                .containsEntry(new TopicPartition("dbz.customers", 0), 42L)
                .containsEntry(new TopicPartition("dbz.orders", 1), 10L)
                .hasSize(2);
    }

    @Test
    void testExplicitTopicOverridesDefault() {
        Map<TopicPartition, Long> offsets =
                KafkaStartupOffsets.parse("topic:other,partition:2,offset:7", "orders");

        Assertions.assertThat(offsets)
                .containsExactly(Assertions.entry(new TopicPartition("other", 2), 7L));
    }

    @Test
    void testMissingTopicWithoutDefaultFails() {
        Assertions.assertThatThrownBy(
                        () -> KafkaStartupOffsets.parse("partition:0,offset:42", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must include 'topic'");
    }

    @Test
    void testMissingPartitionOrOffsetFails() {
        Assertions.assertThatThrownBy(() -> KafkaStartupOffsets.parse("partition:0", "orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition")
                .hasMessageContaining("offset");
    }

    @Test
    void testNonNumericOffsetFails() {
        Assertions.assertThatThrownBy(
                        () -> KafkaStartupOffsets.parse("partition:0,offset:abc", "orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset")
                .hasMessageContaining("abc");
    }

    @Test
    void testUnknownKeyFails() {
        Assertions.assertThatThrownBy(
                        () -> KafkaStartupOffsets.parse("partition:0,offset:1,foo:bar", "orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown key");
    }

    @Test
    void testEmptySpecFails() {
        Assertions.assertThatThrownBy(() -> KafkaStartupOffsets.parse(" ; ; ", "orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one entry");
    }
}
