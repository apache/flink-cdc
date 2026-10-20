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

package io.debezium.relational;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables.TableFilter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RelationalTableFiltersTest {

    @Test
    void testInitialTableFilterIsCached() {
        RelationalTableFilters tableFilters = createTableFilters();

        assertThat(tableFilters.dataCollectionFilter()).isInstanceOf(CachedTableFilter.class);
    }

    @Test
    void testSetDataCollectionFiltersRetainsReplacement() {
        RelationalTableFilters tableFilters = createTableFilters();
        TableFilter replacementFilter = tableId -> false;

        tableFilters.setDataCollectionFilters(replacementFilter);

        assertThat(tableFilters.dataCollectionFilter()).isSameAs(replacementFilter);
    }

    private static RelationalTableFilters createTableFilters() {
        return new RelationalTableFilters(
                Configuration.empty(), tableId -> true, TableId::toString);
    }
}
