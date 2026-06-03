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

package org.apache.flink.cdc.connectors.tdengine.utils;

import org.apache.flink.cdc.connectors.tdengine.serde.TDengineRowData;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/** Tests for {@link TDengineSqlUtils}. */
class TDengineSqlUtilsTest {

    @Test
    void testCreateStableSkipsSubtableField() {
        TDengineTableInfo tableInfo = new TDengineTableInfo("test_db", "metrics");

        String sql =
                TDengineSqlUtils.createStableSql(
                        tableInfo, TDengineTestUtils.SCHEMA, TDengineTestUtils.defaultConfig());

        Assertions.assertThat(sql)
                .isEqualTo(
                        "CREATE STABLE IF NOT EXISTS `test_db`.`metrics` "
                                + "(`ts` TIMESTAMP, `temperature` DOUBLE, `status` NCHAR(256)) "
                                + "TAGS (`location` NCHAR(256))");
        Assertions.assertThat(sql).doesNotContain("device_id");
    }

    @Test
    void testInsertSqlEscapesLiteralsAndRendersBinary() {
        TDengineTableInfo tableInfo = new TDengineTableInfo("test_db", "metrics");
        TDengineRowData first =
                new TDengineRowData(
                        tableInfo,
                        "device_1",
                        1000L,
                        Collections.singletonList("location"),
                        Collections.singletonList("room 'A'"),
                        Arrays.asList("ts", "temperature", "payload"),
                        Arrays.asList(1000L, 12.5D, new byte[] {0x01, 0x02}));
        TDengineRowData second =
                new TDengineRowData(
                        tableInfo,
                        "device_2",
                        2000L,
                        Collections.singletonList("location"),
                        Collections.singletonList("room\\B"),
                        Arrays.asList("ts", "temperature", "payload"),
                        Arrays.asList(2000L, null, new byte[] {(byte) 0xFF}));

        String sql = TDengineSqlUtils.insertSql(tableInfo, Arrays.asList(first, second));

        Assertions.assertThat(sql)
                .isEqualTo(
                        "INSERT INTO `test_db`.`device_1` USING `test_db`.`metrics` "
                                + "TAGS ('room ''A''') (`ts`, `temperature`, `payload`) "
                                + "VALUES (1000, 12.5, 0x0102) "
                                + "`test_db`.`device_2` USING `test_db`.`metrics` "
                                + "TAGS ('room\\\\B') (`ts`, `temperature`, `payload`) "
                                + "VALUES (2000, NULL, 0xFF)");
    }

    @Test
    void testResolveTableInfoUsesMappingBeforeDefaultStable() {
        TDengineTableInfo tableInfo =
                TDengineSqlUtils.resolveTableInfo(
                        TDengineTestUtils.TABLE_ID,
                        TDengineTestUtils.configWithMappings(
                                Collections.singletonMap(TDengineTestUtils.TABLE_ID, "mapped")));

        Assertions.assertThat(tableInfo.getStableName()).isEqualTo("mapped");
    }
}
