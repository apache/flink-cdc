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

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.connectors.maxcompute.utils.cache.UnifiedFileWriter;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/** test utils of UnifiedFileWriter. */
public class UnifiedFileWriterTest {

    @Test
    public void testE2E() throws IOException {
        int testRowCount = 100000;
        UnifiedFileWriter<String> writer =
                new UnifiedFileWriter<>("/tmp/maxcompute_sink_cache", StringSerializer.INSTANCE);
        for (int i = 0; i < testRowCount; i++) {
            writer.write("test");
        }
        writer.close();
        AtomicInteger count = new AtomicInteger();
        writer.read(
                s -> {
                    count.getAndIncrement();
                    return null;
                });
        Assert.assertEquals(testRowCount, count.get());
    }
}
