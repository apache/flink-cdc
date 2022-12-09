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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ObjectUtils}. */
public class ObjectUtilsTest {

    @Test
    public void testMinus() {
        assertEquals(BigDecimal.valueOf(9999), ObjectUtils.minus(10000, 1));
        assertEquals(
                BigDecimal.valueOf(4294967295L),
                ObjectUtils.minus(Integer.MAX_VALUE, Integer.MIN_VALUE));

        assertEquals(BigDecimal.valueOf(9999999999999L), ObjectUtils.minus(10000000000000L, 1L));
        assertEquals(
                new BigDecimal("18446744073709551615"),
                ObjectUtils.minus(Long.MAX_VALUE, Long.MIN_VALUE));

        assertEquals(
                new BigDecimal("99.12344"),
                ObjectUtils.minus(new BigDecimal("100.12345"), new BigDecimal("1.00001")));
    }

    @Test
    public void testCompareByteArray() {
        List<byte[]> list = new ArrayList<>();
        //below data is sorted in ascending order according to the binary column in MySQL
        list.add(toBinary("90c83f1a-b5d0-4333-bb45-b793652a0039"));
        list.add(toBinary("253025c1-47fd-4497-a780-2f067b6b0502"));
        list.add(toBinary("dc92e409-4df0-457e-9e3d-0ac61a5f1517"));
        list.add(toBinary("f09ec2d2-1612-4673-83a4-517292043061"));
        list.add(toBinary("9524fb73-295b-480c-a828-1baf95b9c4e8"));
        list.add(toBinary("13b982e3-4f86-487f-a490-5e9253246865"));
        list.add(toBinary("bcd1c6dc-0845-4a8d-856e-1c2293e7b235"));
        list.add(toBinary("f5a3408f-7974-4c5c-a235-da694806d6e2"));
        list.add(toBinary("f36b0384-437a-4cb9-851e-76530d293513"));
        list.add(toBinary("a71417e1-6990-4de8-b034-92a0f1737d4e"));

        for (int i=0; i<list.size()-1; i++) {
            assertTrue(ObjectUtils.compare(list.get(i), list.get(i + 1)) < 0);
        }
    }

    /**
     * Convert uuid string to binary.
     *
     * @param str uuid string
     * @return uuid bin
     */
    public static byte[] toBinary(String str) {
        UUID uuid = UUID.fromString(str);
        long msb = uuid.getMostSignificantBits();
        msb = msb << 48
            | ((msb >> 16) & 0xFFFFL) << 32
            | msb >>> 32;

        byte[] bytes = new byte[16];
        ByteBuffer.wrap(bytes)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(msb)
            .putLong(uuid.getLeastSignificantBits());
        return bytes;
    }



}
