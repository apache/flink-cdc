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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Unit test for {@link org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils}. */
public class StatementUtilsTest {

    @Test
    void testBuildSplitScanQuery() {
        TableId parse = TableId.parse("db.table");
        RowType splitType =
                RowType.of(
                        new LogicalType[] {DataTypes.INT().getLogicalType()}, new String[] {"id"});
        String allCol = StatementUtils.buildSplitScanQuery(parse, splitType, true, false, "*");
        String excepted = "SELECT * FROM `db`.`table` WHERE `id` <= ? AND NOT (`id` = ?)";
        Assertions.assertThat(allCol).isEqualTo(excepted);

        List<String> projectColList = Arrays.asList("`id`", "`country`");
        String projectCol =
                StatementUtils.buildSplitScanQuery(
                        parse, splitType, true, false, String.join(",", projectColList));
        String exceptedProjectCol =
                "SELECT `id`,`country` FROM `db`.`table` WHERE `id` <= ? AND NOT (`id` = ?)";
        Assertions.assertThat(projectCol).isEqualTo(exceptedProjectCol);
    }
}
