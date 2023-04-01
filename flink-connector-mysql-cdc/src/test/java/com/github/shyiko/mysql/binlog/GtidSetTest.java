package com.github.shyiko.mysql.binlog;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link GtidSet}.
 */
public class GtidSetTest {

    @Test
    public void overwriteThenAddTest() {
        GtidSet gtidSet = new GtidSet("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-88");

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:100");
        assertEquals("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-100", gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:101");
        assertEquals("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101", gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:103");
        assertEquals("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101:103-103", gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:104");
        assertEquals("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101:103-104", gtidSet.toString());
    }

    @Test
    public void overwriteThenAddInEmptyGtidSetTest() {
        GtidSet gtidSet = new GtidSet("");

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:100");
        assertEquals("85ebef13-1e2d-11e7-a18e-080027d89544:1-100", gtidSet.toString());
    }

    @Test
    public void overwriteThenAddInBiggerInitGtidSetTest() {
        GtidSet gtidSet = new GtidSet("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-2147483647");

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:100");
        assertEquals("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-100", gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:101");
        assertEquals("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101", gtidSet.toString());

        gtidSet.overwriteThenAdd("85ebef13-1e2d-11e7-a18e-080027d89544:1000");
        assertEquals("6f7b6d88-6a12-11e9-bd82-0242ac110002:1-39816543,85ebef13-1e2d-11e7-a18e-080027d89544:1-101:1000-1000", gtidSet.toString());
    }

}
