/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.utils;

import org.junit.Test;

import java.math.BigInteger;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.splitKeyRangeContains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RecordUtils}. */
public class RecordUtilsTest {

    @Test
    public void testSplitKeyRangeContains() {
        // table with only one split
        assertTrue(splitKeyRangeContains(new Object[] {100L}, null, null));

        // the last split
        assertTrue(splitKeyRangeContains(new Object[] {101L}, new Object[] {100L}, null));

        // the first split
        assertTrue(splitKeyRangeContains(new Object[] {101L}, null, new Object[] {1024L}));

        // general splits
        assertTrue(
                splitKeyRangeContains(
                        new Object[] {100L}, new Object[] {1L}, new Object[] {1024L}));
        assertFalse(
                splitKeyRangeContains(new Object[] {0L}, new Object[] {1L}, new Object[] {1024L}));

        // split key from binlog may have different type
        assertTrue(
                splitKeyRangeContains(
                        new Object[] {BigInteger.valueOf(100L)},
                        new Object[] {1L},
                        new Object[] {1024L}));
        assertFalse(
                splitKeyRangeContains(
                        new Object[] {BigInteger.valueOf(0L)},
                        new Object[] {1L},
                        new Object[] {1024L}));
    }
}
