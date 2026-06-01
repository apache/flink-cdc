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

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import io.debezium.relational.Tables.TableFilter;

/** A bounded cache for table filter results. */
public class CachedTableFilter implements TableFilter {

    private static final long TABLE_FILTER_CACHE_MAXIMUM_SIZE = 1024;

    private final TableFilter rawTableFilter;
    private final LoadingCache<TableId, Boolean> tableFilterCache;

    private CachedTableFilter(TableFilter rawTableFilter) {
        this.rawTableFilter = rawTableFilter;
        this.tableFilterCache =
                CacheBuilder.newBuilder()
                        .maximumSize(TABLE_FILTER_CACHE_MAXIMUM_SIZE)
                        .build(
                                new CacheLoader<TableId, Boolean>() {
                                    @Override
                                    public Boolean load(TableId tableId) {
                                        return rawTableFilter.isIncluded(tableId);
                                    }
                                });
    }

    /** Wraps the given filter in a cache, or returns it unchanged if it is already cached. */
    public static CachedTableFilter from(TableFilter tableFilter) {
        if (tableFilter instanceof CachedTableFilter) {
            return (CachedTableFilter) tableFilter;
        }
        return new CachedTableFilter(tableFilter);
    }

    /** Returns a new cached filter that also requires the additional filter to match. */
    public CachedTableFilter withAdditionalFilter(TableFilter additionalFilter) {
        TableFilter rawFilter = rawTableFilter;
        return new CachedTableFilter(
                tableId -> rawFilter.isIncluded(tableId) && additionalFilter.isIncluded(tableId));
    }

    @Override
    public boolean isIncluded(TableId tableId) {
        return tableFilterCache.getUnchecked(tableId);
    }
}
