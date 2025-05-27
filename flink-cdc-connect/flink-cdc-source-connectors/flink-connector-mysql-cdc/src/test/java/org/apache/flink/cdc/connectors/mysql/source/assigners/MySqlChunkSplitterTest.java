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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.cdc.connectors.mysql.schema.MySqlSchema;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.List;

/** Tests for {@link org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlChunkSplitter}. */
class MySqlChunkSplitterTest {

    @Test
    void testSplitEvenlySizedChunksOverflow() {
        MySqlSourceConfig sourceConfig =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList("")
                        .tableList("")
                        .hostname("")
                        .username("")
                        .password("")
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .assignUnboundedChunkFirst(false)
                        .createConfig(0);
        MySqlChunkSplitter splitter = new MySqlChunkSplitter(null, sourceConfig);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 19,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        Assertions.assertThat(res)
                .containsExactly(ChunkRange.of(null, 2147483638), ChunkRange.of(2147483638, null));
    }

    @Test
    void testSplitEvenlySizedChunksNormal() {
        MySqlSourceConfig sourceConfig =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList("")
                        .tableList("")
                        .hostname("")
                        .username("")
                        .password("")
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .assignUnboundedChunkFirst(false)
                        .createConfig(0);
        MySqlChunkSplitter splitter = new MySqlChunkSplitter(null, sourceConfig);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 20,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        Assertions.assertThat(res)
                .containsExactly(
                        ChunkRange.of(null, 2147483637),
                        ChunkRange.of(2147483637, 2147483647),
                        ChunkRange.of(2147483647, null));
    }

    @Test
    void testIgnoreNoPrimaryKeyTable() throws Exception {
        // 创建配置，设置ignoreNoPrimaryKeyTable为true
        MySqlSourceConfig sourceConfig =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList("test_db")
                        .tableList("test_db.test_table")
                        .hostname("localhost")
                        .username("test")
                        .password("test")
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .ignoreNoPrimaryKeyTable(true)
                        .createConfig(0);

        // 创建一个简单的MySqlSchema实现
        MySqlSchema schema =
                new MySqlSchema(sourceConfig, true) {
                    @Override
                    public TableChanges.TableChange getTableSchema(
                            MySqlPartition partition, JdbcConnection jdbc, TableId tableId) {
                        // 创建一个没有主键的表
                        Table noPkTable =
                                Table.editor()
                                        .tableId(tableId)
                                        .addColumn(
                                                Column.editor()
                                                        .name("id")
                                                        .type("BIGINT")
                                                        .jdbcType(-5)
                                                        .optional(false)
                                                        .create())
                                        .create();
                        return new TableChanges.TableChange(
                                TableChanges.TableChangeType.CREATE, noPkTable);
                    }
                };

        MySqlChunkSplitter splitter = new MySqlChunkSplitter(schema, sourceConfig);
        MySqlPartition partition = new MySqlPartition("mysql_binlog_source");

        // 测试无主键表
        List<MySqlSnapshotSplit> splits =
                splitter.splitChunks(partition, new TableId("test_db", null, "test_table"));

        // 验证对于没有主键的表，返回空的分片列表
        Assertions.assertThat(splits).isEmpty();
    }
}
