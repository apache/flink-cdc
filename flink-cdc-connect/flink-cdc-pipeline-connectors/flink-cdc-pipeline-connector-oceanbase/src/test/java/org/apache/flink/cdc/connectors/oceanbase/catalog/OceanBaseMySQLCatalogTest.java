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

package org.apache.flink.cdc.connectors.oceanbase.catalog;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests for {@link OceanBaseMySQLCatalogTest}. */
public class OceanBaseMySQLCatalogTest {

    private static final ImmutableMap<String, String> configMap =
            ImmutableMap.<String, String>builder()
                    .put("url", "localhost")
                    .put("username", "test")
                    .put("password", "test")
                    .build();

    @Test
    void testBuildAlterAddColumnsSql() {
        OceanBaseMySQLCatalog oceanBaseCatalog =
                new OceanBaseMySQLCatalog(new OceanBaseConnectorOptions(configMap));

        List<OceanBaseColumn> addColumns = Lists.newArrayList();
        addColumns.add(
                new OceanBaseColumn.Builder()
                        .setColumnName("age")
                        .setOrdinalPosition(-1)
                        .setColumnComment("age")
                        .setDataType("varchar(10)")
                        .build());
        String columnsSql = oceanBaseCatalog.buildAlterAddColumnsSql("test", "test", addColumns);
        Assertions.assertThat(columnsSql)
                .isEqualTo(
                        "ALTER TABLE `test`.`test` ADD COLUMN `age` VARCHAR(10) NULL COMMENT \"age\";");
    }
}
