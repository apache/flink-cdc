/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.splitKeyRangeContains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RecordUtils}. */
public class RecordUtilsTest {

    @Test
    public void testSplitKeyRangeContains() {
        // table with only one split
        assertKeyRangeContains(new Object[] {100L}, null, null);
        // the last split
        assertKeyRangeContains(new Object[] {101L}, new Object[] {100L}, null);

        // the first split
        assertKeyRangeContains(new Object[] {101L}, null, new Object[] {1024L});

        // general splits
        assertKeyRangeContains(new Object[] {100L}, new Object[] {1L}, new Object[] {1024L});
        assertFalse(
                splitKeyRangeContains(new Object[] {0L}, new Object[] {1L}, new Object[] {1024L}));

        // split key from binlog may have different type
        assertKeyRangeContains(
                new Object[] {BigInteger.valueOf(100L)}, new Object[] {1L}, new Object[] {1024L});
        assertFalse(
                splitKeyRangeContains(
                        new Object[] {BigInteger.valueOf(0L)},
                        new Object[] {1L},
                        new Object[] {1024L}));
    }

    @Test
    public void testDifferentKeyTypes() {
        // first split
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Byte.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Short.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Integer.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Long.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {BigInteger.valueOf(6)});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {BigDecimal.valueOf(6)});

        // other splits
        assertKeyRangeContains(
                new Object[] {Byte.valueOf("6")},
                new Object[] {Byte.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Short.valueOf("60")},
                new Object[] {Short.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Integer.valueOf("600")},
                new Object[] {Integer.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Long.valueOf("6000")},
                new Object[] {Long.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {BigInteger.valueOf(60000)},
                new Object[] {BigInteger.valueOf(6)},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {BigDecimal.valueOf(60000)},
                new Object[] {BigDecimal.valueOf(6)},
                new Object[] {BigDecimal.valueOf(100000L)});

        // last split
        assertKeyRangeContains(new Object[] {7}, new Object[] {Byte.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Short.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Integer.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Long.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {BigInteger.valueOf(6)}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {BigDecimal.valueOf(6)}, null);
    }

    private void assertKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        assertTrue(splitKeyRangeContains(key, splitKeyStart, splitKeyEnd));
    }
}
