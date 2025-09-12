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

package org.apache.flink.cdc.connectors.base.source.meta.split;

import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.Function;

/** Unit tests for {@link FinishedSnapshotSplitInfo}. */
class FinishedSnapshotSplitInfoTest {
    @Test
    void testInfosWithBinaryPrimaryKeyAreEqual() {
        assertInfosWithBinaryPrimaryKeyAreEqual(Function.identity());
    }

    @Test
    void testInfosWithBinaryPrimaryKeyHaveEqualHashCodes() {
        assertInfosWithBinaryPrimaryKeyAreEqual(FinishedSnapshotSplitInfo::hashCode);
    }

    @Test
    void testInfosWithBinaryPrimaryKeyHaveEqualStringRepresentations() {
        assertInfosWithBinaryPrimaryKeyAreEqual(FinishedSnapshotSplitInfo::toString);
    }

    private <R> void assertInfosWithBinaryPrimaryKeyAreEqual(
            Function<FinishedSnapshotSplitInfo, R> function) {
        FinishedSnapshotSplitInfo original =
                new FinishedSnapshotSplitInfo(
                        TableId.parse("table"),
                        "split-1",
                        new Object[] {new byte[] {0x01, 0x02}},
                        new Object[] {new byte[] {0x03, 0x04}},
                        null,
                        Mockito.mock(OffsetFactory.class));

        FinishedSnapshotSplitInfo copy = original.deserialize(original.serialize());

        Assertions.assertThat(function.apply(copy)).isEqualTo(function.apply(original));
    }
}
