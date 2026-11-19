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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for {@link StreamSplit}. */
class StreamSplitTest {

    @Test
    void testDuplicateFinishedSnapshotSplitInfosAreRejected() {
        FinishedSnapshotSplitInfo info =
                new FinishedSnapshotSplitInfo(
                        new TableId("catalog", "schema", "table"),
                        "split",
                        null,
                        null,
                        null,
                        mock(OffsetFactory.class));
        List<FinishedSnapshotSplitInfo> infos = new ArrayList<>();
        infos.add(info);
        infos.add(info);

        assertThatThrownBy(
                        () ->
                                new StreamSplit(
                                        "stream-split",
                                        null,
                                        null,
                                        infos,
                                        Collections.emptyMap(),
                                        0,
                                        false,
                                        false))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }
}
