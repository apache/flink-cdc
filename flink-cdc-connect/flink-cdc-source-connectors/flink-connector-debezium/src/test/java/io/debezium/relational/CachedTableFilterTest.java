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

import io.debezium.relational.Tables.TableFilter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class CachedTableFilterTest {

    @Test
    void testCachesPositiveAndNegativeResultsForEquivalentTableIds() {
        AtomicInteger invocationCount = new AtomicInteger();
        CachedTableFilter cachedTableFilter =
                CachedTableFilter.from(
                        tableId -> {
                            invocationCount.incrementAndGet();
                            return tableId.table().startsWith("orders");
                        });

        TableId includedTable = new TableId("test_db", null, "orders_1");
        TableId sameIncludedTable = new TableId("test_db", null, "orders_1");
        TableId unmatchedTable = new TableId("test_db", null, "customers");
        TableId sameUnmatchedTable = new TableId("test_db", null, "customers");

        assertThat(cachedTableFilter.isIncluded(includedTable)).isTrue();
        assertThat(cachedTableFilter.isIncluded(sameIncludedTable)).isTrue();
        assertThat(cachedTableFilter.isIncluded(unmatchedTable)).isFalse();
        assertThat(cachedTableFilter.isIncluded(sameUnmatchedTable)).isFalse();
        assertThat(invocationCount).hasValue(2);
    }

    @Test
    void testFromReturnsCachedFilterUnchanged() {
        CachedTableFilter cachedTableFilter = CachedTableFilter.from(tableId -> true);

        assertThat(CachedTableFilter.from(cachedTableFilter)).isSameAs(cachedTableFilter);
    }

    @Test
    void testAdditionalFilterCreatesSingleCacheOverRawDelegate() {
        AtomicInteger rawFilterInvocationCount = new AtomicInteger();
        AtomicInteger additionalFilterInvocationCount = new AtomicInteger();
        CachedTableFilter cachedTableFilter =
                CachedTableFilter.from(
                        tableId -> {
                            rawFilterInvocationCount.incrementAndGet();
                            return true;
                        });
        TableId tableId = new TableId("test_db", null, "orders_1");

        assertThat(cachedTableFilter.isIncluded(tableId)).isTrue();

        CachedTableFilter combinedFilter =
                cachedTableFilter.withAdditionalFilter(
                        ignored -> {
                            additionalFilterInvocationCount.incrementAndGet();
                            return false;
                        });

        assertThat(combinedFilter).isNotSameAs(cachedTableFilter);
        assertThat(combinedFilter.isIncluded(tableId)).isFalse();
        assertThat(combinedFilter.isIncluded(tableId)).isFalse();
        assertThat(rawFilterInvocationCount).hasValue(2);
        assertThat(additionalFilterInvocationCount).hasValue(1);

        assertThat(cachedTableFilter.isIncluded(tableId)).isTrue();
        assertThat(rawFilterInvocationCount).hasValue(2);
    }

    @Test
    void testCacheSizeIsBounded() {
        AtomicInteger invocationCount = new AtomicInteger();
        TableFilter cachedTableFilter =
                CachedTableFilter.from(
                        tableId -> {
                            invocationCount.incrementAndGet();
                            return true;
                        });
        List<TableId> tableIds = new ArrayList<>();
        for (int i = 0; i < 1025; i++) {
            tableIds.add(new TableId("test_db", null, "table_" + i));
        }

        tableIds.forEach(cachedTableFilter::isIncluded);
        assertThat(invocationCount).hasValue(1025);

        tableIds.forEach(cachedTableFilter::isIncluded);
        assertThat(invocationCount).hasValueGreaterThan(1025);
    }
}
