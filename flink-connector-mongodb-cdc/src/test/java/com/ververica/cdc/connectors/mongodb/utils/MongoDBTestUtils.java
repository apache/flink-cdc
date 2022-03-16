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

package com.ververica.cdc.connectors.mongodb.utils;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/** MongoDB test utilities. */
public class MongoDBTestUtils {

    public static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            Thread.sleep(100);
        }
    }

    public static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        waitForSinkSize(sinkName, expectedSize, 10, TimeUnit.MINUTES);
    }

    public static void waitForSinkSize(
            String sinkName, int expectedSize, long timeout, TimeUnit timeUnit)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
        while (sinkSize(sinkName) < expectedSize) {
            if (System.nanoTime() > deadline) {
                fail(
                        "Wait for sink size timeout, raw results: \n"
                                + String.join(
                                        "\n", TestValuesTableFactory.getRawResults(sinkName)));
            }
            Thread.sleep(100);
        }
    }

    public static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }
}
