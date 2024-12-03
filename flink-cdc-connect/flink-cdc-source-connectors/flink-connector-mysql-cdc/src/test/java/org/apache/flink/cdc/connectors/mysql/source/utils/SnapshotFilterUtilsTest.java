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

import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Unit test for {@link org.apache.flink.cdc.connectors.mysql.source.utils.SnapshotFilterUtils}. */
public class SnapshotFilterUtilsTest {

    @Test
    public void test() {
        Map<String, String> map = new HashMap<>();
        map.put("db.user", "id > 100");
        map.put("db.order_[0-9]+", "id > 200");
        assertEquals(
                "id > 100", SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.user")));
        assertEquals(
                "id > 200",
                SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.order_1")));
        assertEquals(
                "id > 200",
                SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.order_2")));
        assertNull(SnapshotFilterUtils.getSnapshotFilter(map, TableId.parse("db.shop")));
    }
}
