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

package org.apache.flink.cdc.connectors.mysql.source.config;

import org.apache.flink.cdc.connectors.mysql.source.utils.SnapshotFilterUtils;

import io.debezium.relational.TableId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MySqlSourceConfigFactory}. */
class MySqlSourceConfigFactoryTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSnapshotFilterOrder(boolean releaseSnapshotMetadataEnabled) {
        Map<String, String> filters = new LinkedHashMap<>();
        filters.put("db.order_[0-9]+", "id > 100");
        filters.put("db.order_1", "id > 200");
        MySqlSourceConfigFactory factory =
                createFactory()
                        .releaseSnapshotMetadataEnabled(releaseSnapshotMetadataEnabled)
                        .snapshotFilters(filters);
        MySqlSourceConfig broadFirst = factory.createConfig(0);
        MySqlSourceConfig specificFirst =
                createFactory()
                        .releaseSnapshotMetadataEnabled(releaseSnapshotMetadataEnabled)
                        .snapshotFilters("db.order_1", "id > 200")
                        .snapshotFilters("db.order_[0-9]+", "id > 100")
                        .createConfig(0);

        assertThat(broadFirst.getSnapshotFilters().keySet())
                .containsExactly("db.order_[0-9]+", "db.order_1");
        assertThat(specificFirst.getSnapshotFilters().keySet())
                .containsExactly("db.order_1", "db.order_[0-9]+");
        assertThat(broadFirst.isReleaseSnapshotMetadataEnabled())
                .isEqualTo(releaseSnapshotMetadataEnabled);
        assertThat(specificFirst.isReleaseSnapshotMetadataEnabled())
                .isEqualTo(releaseSnapshotMetadataEnabled);
        TableId tableId = TableId.parse("db.order_1");
        assertThat(SnapshotFilterUtils.getSnapshotFilter(broadFirst.getSnapshotFilters(), tableId))
                .isEqualTo("id > 100");
        assertThat(
                        SnapshotFilterUtils.getSnapshotFilter(
                                specificFirst.getSnapshotFilters(), tableId))
                .isEqualTo("id > 200");

        // Creating a config snapshots the rules instead of retaining the mutable factory map.
        factory.snapshotFilters("db.order_[0-9]+", "id > 300");
        assertThat(SnapshotFilterUtils.getSnapshotFilter(broadFirst.getSnapshotFilters(), tableId))
                .isEqualTo("id > 100");
        assertThatThrownBy(() -> broadFirst.getSnapshotFilters().put("db.other", "id > 400"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private MySqlSourceConfigFactory createFactory() {
        return new MySqlSourceConfigFactory()
                .hostname("localhost")
                .username("user")
                .password("password")
                .databaseList("db")
                .tableList("db.order_1")
                .serverTimeZone("UTC");
    }
}
