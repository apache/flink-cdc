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

package org.apache.flink.cdc.common.configuration;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link MemorySize} */
public class MemorySizeTest {

    @Test
    public void testConstructor() {
        MemorySize size = new MemorySize(1024);
        assertEquals(1024, size.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNegative() {
        new MemorySize(-1);
    }

    @Test
    public void testOfMebiBytes() {
        MemorySize size = MemorySize.ofMebiBytes(1);
        assertEquals(1024 * 1024, size.getBytes());
    }

    @Test
    public void testGetters() {
        MemorySize size = new MemorySize(1024 * 1024 * 1024); // 1 GiB
        assertEquals(1024 * 1024 * 1024, size.getBytes());
        assertEquals(1024 * 1024, size.getKibiBytes());
        assertEquals(1024, size.getMebiBytes());
        assertEquals(1, size.getGibiBytes());
        assertEquals(0, size.getTebiBytes());
    }

    @Test
    public void testToStringAndToHumanReadableString() {
        MemorySize size = new MemorySize(1024);
        assertEquals("1 kb", size.toString());
        assertEquals("1024 bytes", size.toHumanReadableString());
    }

    @Test
    public void testAddSubtractMultiplyDivide() {
        MemorySize size1 = new MemorySize(2048);
        MemorySize size2 = new MemorySize(1024);

        MemorySize resultAdd = size1.add(size2);
        assertEquals(3072, resultAdd.getBytes());

        MemorySize resultSubtract = size1.subtract(size2);
        assertEquals(1024, resultSubtract.getBytes());

        MemorySize resultMultiply = size2.multiply(2);
        assertEquals(2048, resultMultiply.getBytes());

        MemorySize resultDivide = size1.divide(2);
        assertEquals(1024, resultDivide.getBytes());
    }

    @Test
    public void testParse() {
        assertEquals(1024, MemorySize.parse("1k").getBytes());
        assertEquals(1024 * 1024, MemorySize.parse("1m").getBytes());
        assertEquals(1024 * 1024 * 1024, MemorySize.parse("1g").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNegative() {
        MemorySize.parse("-1k");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidUnit() {
        MemorySize.parse("1x");
    }
}
