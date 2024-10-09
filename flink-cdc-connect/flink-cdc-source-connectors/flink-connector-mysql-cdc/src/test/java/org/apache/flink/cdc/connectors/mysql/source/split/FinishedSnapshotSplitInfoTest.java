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

package org.apache.flink.cdc.connectors.mysql.source.split;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link FinishedSnapshotSplitInfo}. */
public class FinishedSnapshotSplitInfoTest {

    @Test
    public void testCompareTo() {
        Object[][] baseSplit = new Object[][] {new Object[] {100}, new Object[] {199}};
        FinishedSnapshotSplitInfo baseSplitInfo =
                new FinishedSnapshotSplitInfo(null, "", baseSplit[0], baseSplit[1], null);

        Object[][] thatSplit = new Object[][] {null, new Object[] {99}};
        FinishedSnapshotSplitInfo thatSplitInfo =
                new FinishedSnapshotSplitInfo(null, "", thatSplit[0], thatSplit[1], null);
        Assertions.assertTrue(baseSplitInfo.compareTo(thatSplitInfo) > 0);

        thatSplit = new Object[][] {new Object[] {200}, new Object[] {299}};
        thatSplitInfo = new FinishedSnapshotSplitInfo(null, "", thatSplit[0], thatSplit[1], null);
        Assertions.assertTrue(baseSplitInfo.compareTo(thatSplitInfo) < 0);

        thatSplit = new Object[][] {new Object[] {300}, null};
        thatSplitInfo = new FinishedSnapshotSplitInfo(null, "", thatSplit[0], thatSplit[1], null);
        Assertions.assertTrue(baseSplitInfo.compareTo(thatSplitInfo) < 0);
    }
}
