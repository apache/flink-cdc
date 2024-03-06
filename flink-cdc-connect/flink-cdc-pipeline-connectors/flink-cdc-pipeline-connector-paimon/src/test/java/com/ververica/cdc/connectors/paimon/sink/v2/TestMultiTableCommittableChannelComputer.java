/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.paimon.sink.v2;

import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A test for {@link MultiTableCommittableChannelComputer}. */
public class TestMultiTableCommittableChannelComputer {

    @Test
    public void testChannel() {
        MultiTableCommittableChannelComputer computer = new MultiTableCommittableChannelComputer();
        computer.setup(4);
        List<MultiTableCommittable> commits =
                Arrays.asList(
                        new MultiTableCommittable("database", "table1", 1L, null, null),
                        new MultiTableCommittable("database", "table2", 1L, null, null),
                        new MultiTableCommittable("database", "table1", 1L, null, null),
                        new MultiTableCommittable("database", "table5", 1L, null, null),
                        new MultiTableCommittable("database", "table3", 1L, null, null),
                        new MultiTableCommittable("database", "table8", 1L, null, null),
                        new MultiTableCommittable("database", "table5", 1L, null, null),
                        new MultiTableCommittable("database", "table1", 1L, null, null),
                        new MultiTableCommittable("database", "table9", 1L, null, null),
                        new MultiTableCommittable("database", "table5", 1L, null, null),
                        new MultiTableCommittable("database", "table3", 1L, null, null),
                        new MultiTableCommittable("database", "table8", 1L, null, null));
        Map<Integer, Set<String>> map = new HashMap<>();
        commits.forEach(
                (commit) -> {
                    int channel = computer.channel(new CommittableWithLineage<>(commit, 1L, 0));
                    Set<String> set = map.getOrDefault(channel, new HashSet<>());
                    set.add(commit.getTable());
                    map.put(channel, set);
                });
        int count = 0;
        for (Map.Entry<Integer, Set<String>> entry : map.entrySet()) {
            count += entry.getValue().size();
        }
        // Not a table is appeared in more than one channel.
        Assertions.assertEquals(6, count);
    }
}
