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

package org.apache.flink.cdc.connectors.mysql.testutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.CreateTableEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Test utilities for MySQL event source. */
public class MySqSourceTestUtils {

    public static final String TEST_USER = "mysqluser";
    public static final String TEST_PASSWORD = "mysqlpw";

    public static <T> List<T> fetchResults(Iterator<T> iter, int size) {
        List<T> result = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            result.add(event);
            size--;
        }
        return result;
    }

    public static <T> Tuple2<List<T>, List<CreateTableEvent>> fetchResultsAndCreateTableEvent(
            Iterator<T> iter, int size) {
        List<T> result = new ArrayList<>(size);
        List<CreateTableEvent> createTableEvents = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (event instanceof CreateTableEvent) {
                createTableEvents.add((CreateTableEvent) event);
            } else {
                result.add(event);
                size--;
            }
        }
        return Tuple2.of(result, createTableEvents);
    }

    public static String getServerId(int parallelism) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + parallelism);
    }

    public static void loopCheck(
            Supplier<Boolean> runnable, String description, Duration timeout, Duration interval)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (runnable.get()) {
                return;
            }
            Thread.sleep(interval.toMillis());
        }
        throw new TimeoutException(
                "Ran out of time when waiting for " + description + " to success.");
    }

    private MySqSourceTestUtils() {}
}
