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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PatternCache}. */
class PatternCacheTest {

    @AfterEach
    void tearDown() {
        PatternCache.clear();
    }

    @Test
    void testCacheHit() {
        String regex = "aia_t_icc_jjdb_\\d{6}";

        Pattern pattern1 = PatternCache.getPattern(regex);
        Pattern pattern2 = PatternCache.getPattern(regex);

        // Should return the same instance.
        assertThat(pattern1).isSameAs(pattern2);
        assertThat(PatternCache.size()).isEqualTo(1);
    }

    @Test
    void testMultiplePatterns() {
        Pattern pattern1 = PatternCache.getPattern("regex1");
        Pattern pattern2 = PatternCache.getPattern("regex2");
        Pattern pattern3 = PatternCache.getPattern("regex1");

        assertThat(pattern1).isSameAs(pattern3);
        assertThat(pattern1).isNotSameAs(pattern2);
        assertThat(PatternCache.size()).isEqualTo(2);
    }

    @Test
    void testLruEvictionWhenCacheExceedsMaxSize() throws Exception {
        int maxCacheSize =
PatternCache.MAX_CACHE_SIZE;

        Pattern pattern0 = PatternCache.getPattern("regex0");
        Pattern pattern1 = PatternCache.getPattern("regex1");
        for (int i = 2; i < maxCacheSize; i++) {
            PatternCache.getPattern("regex" + i);
        }
        assertThat(PatternCache.size()).isEqualTo(maxCacheSize);

        assertThat(PatternCache.getPattern("regex0")).isSameAs(pattern0);

        PatternCache.getPattern("regex" + maxCacheSize);

        assertThat(PatternCache.size()).isEqualTo(maxCacheSize);
        assertThat(PatternCache.getPattern("regex0")).isSameAs(pattern0);
        assertThat(PatternCache.getPattern("regex1")).isNotSameAs(pattern1);
    }

    @Test
    void testClear() {
        PatternCache.getPattern("regex1");
        PatternCache.getPattern("regex2");
        assertThat(PatternCache.size()).isEqualTo(2);

        PatternCache.clear();
        assertThat(PatternCache.size()).isEqualTo(0);
    }

    @Test
    void testPatternMatching() {
        Pattern pattern = PatternCache.getPattern("aia_t_icc_jjdb_\\d{6}");

        assertThat(pattern.matcher("aia_t_icc_jjdb_202512").matches()).isTrue();
        assertThat(pattern.matcher("aia_t_icc_jjdb_abc").matches()).isFalse();
    }
}
