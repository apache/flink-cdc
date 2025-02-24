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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.apache.iceberg.io.WriteResult;
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
        WriteResult writeResult = WriteResult.builder().addDataFiles().addDeleteFiles().build();
        List<WriteResultWrapper> commits =
                Arrays.asList(
                        new WriteResultWrapper(writeResult, TableId.tableId("table2")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table1")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table5")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table1")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table9")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table3")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table8")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table2")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table5")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table2")),
                        new WriteResultWrapper(writeResult, TableId.tableId("table2")));
        Map<Integer, Set<String>> map = new HashMap<>();
        commits.forEach(
                (commit) -> {
                    int channel = computer.channel(new CommittableWithLineage<>(commit, 1L, 0));
                    Set<String> set = map.getOrDefault(channel, new HashSet<>());
                    set.add(commit.getTableId().toString());
                    map.put(channel, set);
                });

        Set<String> actualtables = new HashSet<>();
        for (Map.Entry<Integer, Set<String>> entry : map.entrySet()) {
            actualtables.addAll(entry.getValue());
        }
        Set<String> expectedTables =
                new HashSet<>(
                        Arrays.asList("table1", "table2", "table3", "table5", "table8", "table9"));
        // Not a table is appeared in more than one channel.
        Assertions.assertEquals(actualtables, expectedTables);
    }
}
