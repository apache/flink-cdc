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

package org.apache.flink.cdc.common.data.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link BinaryArrayData}. */
public class BinaryArrayDataTest {

    @Test
    public void testBinaryArrayDataInitialization() {
        // Create a sample memory segment with dummy data
        byte[] data = new byte[256];
        MemorySegment segment = MemorySegmentFactory.wrap(data);

        // Write the number of elements at the beginning
        segment.putInt(0, 5);

        // Initialize BinaryArrayData with sample data
        BinaryArrayData binaryArrayData = new BinaryArrayData();
        binaryArrayData.pointTo(new MemorySegment[] {segment}, 0, data.length);

        // Verify the size
        Assertions.assertEquals(5, binaryArrayData.size());

        // Write and read values
        binaryArrayData.setInt(0, 42);
        Assertions.assertEquals(42, binaryArrayData.getInt(0));

        binaryArrayData.setLong(1, 123456789L);
        Assertions.assertEquals(123456789L, binaryArrayData.getLong(1));

        binaryArrayData.setBoolean(2, true);
        Assertions.assertTrue(binaryArrayData.getBoolean(2));

        //        binaryArrayData.setString(3, "test");
        //        assertEquals("test", binaryArrayData.getString(3).toString());
        //
        //        // Test for out-of-bounds access
        //        assertThrows(IndexOutOfBoundsException.class, () -> binaryArrayData.getInt(100));
    }
}
