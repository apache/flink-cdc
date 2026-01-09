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

package org.apache.flink.cdc.common.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * LRU cache for compiled {@link Pattern} instances.
 *
 * <p>It avoids repeated {@link Pattern#compile(String)} calls for the same regex.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Pattern pattern = PatternCache.getPattern("aia_t_icc_jjdb_\\d{6}");
 * Matcher matcher = pattern.matcher(tableName);
 * }</pre>
 */
public class PatternCache {

    /** Maximum number of cached patterns. */
    private static final int MAX_CACHE_SIZE = 100;

    /** Pattern cache with LRU eviction. */
    private static final Map<String, Pattern> CACHE =
            new LinkedHashMap<String, Pattern>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Pattern> eldest) {
                    return size() > MAX_CACHE_SIZE;
                }
            };

    /**
     * Returns a cached {@link Pattern} or compiles and caches it.
     *
     * @param regex regular expression
     * @return compiled {@link Pattern}
     */
    public static synchronized Pattern getPattern(String regex) {
        return CACHE.computeIfAbsent(regex, Pattern::compile);
    }

    /** Clears the cache (mainly for tests). */
    public static synchronized void clear() {
        CACHE.clear();
    }

    /** Returns the current cache size. */
    public static synchronized int size() {
        return CACHE.size();
    }
}
