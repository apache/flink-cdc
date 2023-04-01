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

package com.github.shyiko.mysql.binlog;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Unit test for {@link GtidSet}. */
public class GtidSetTest {

    @Test
    public void testOverwriteThenAdd() {
        GtidSet gtidSet =
                new GtidSet(
                        "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-88");

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:100");
        assertEquals(
                "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-100",
                gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:101");
        assertEquals(
                "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101",
                gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:103");
        assertEquals(
                "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101:103-103",
                gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:104");
        assertEquals(
                "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101:103-104",
                gtidSet.toString());
    }

    @Test
    public void testOverwriteThenAddInEmptyGtidSet() {
        GtidSet gtidSet = new GtidSet("");

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:100");
        assertEquals("85ebef13-1e2d-11e7-a18e-080027d89544:1-100", gtidSet.toString());
    }

    @Test
    public void testOverwriteThenAddInBiggerInitGtidSet() {
        GtidSet gtidSet =
                new GtidSet(
                        "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-2147483647");

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:100");
        assertEquals(
                "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-100",
                gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:101");
        assertEquals(
                "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101",
                gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:1000");
        assertEquals(
                "6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101:1000-1000",
                gtidSet.toString());
    }
}
