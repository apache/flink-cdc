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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MySqlTableDiscoveryTest {
    @Test
    void appliesConfiguredCaptureAndExclusionFiltersToMetadata() {
        MySqlSourceConfigFactory factory =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .username("test")
                        .password("test")
                        .databaseList("sales")
                        .tableList(".*\\.orders.*")
                        .excludeTableList("sales.orders_archive")
                        .serverTimeZone("UTC");
        MySqlDataSource source =
                new MySqlDataSource(factory) {
                    @Override
                    public MetadataAccessor getMetadataAccessor() {
                        return new MetadataAccessor() {
                            @Override
                            public List<String> listNamespaces() {
                                throw new AssertionError();
                            }

                            @Override
                            public List<String> listSchemas(String namespace) {
                                throw new AssertionError();
                            }

                            @Override
                            public Schema getTableSchema(TableId table) {
                                throw new AssertionError();
                            }

                            @Override
                            public List<TableId> listTables(String namespace, String schema) {
                                return Arrays.asList(
                                        TableId.parse("sales.orders_1"),
                                        TableId.parse("sales.orders_2"),
                                        TableId.parse("sales.orders_archive"),
                                        TableId.parse("sales.customers"),
                                        TableId.parse("other.orders_1"));
                            }
                        };
                    }
                };
        assertThat(source.listCapturedTables())
                .containsExactly(TableId.parse("sales.orders_1"), TableId.parse("sales.orders_2"));
    }
}
