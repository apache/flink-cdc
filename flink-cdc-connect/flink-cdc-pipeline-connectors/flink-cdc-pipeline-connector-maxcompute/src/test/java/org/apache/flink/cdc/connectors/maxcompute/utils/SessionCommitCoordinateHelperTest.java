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

import org.apache.flink.cdc.connectors.maxcompute.common.Constant;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** */
class SessionCommitCoordinateHelperTest {

    @Test
    void testSessionCommit() {
        SessionCommitCoordinateHelper sessionCommitCoordinateHelper =
                new SessionCommitCoordinateHelper(4);
        sessionCommitCoordinateHelper.clear();
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        List<Future<?>> futures = new ArrayList<>();
        futures.add(
                executorService.submit(
                        () -> {
                            int expect = 1;
                            while (sessionCommitCoordinateHelper.isCommitting()) {
                                String toCommitSessionId =
                                        sessionCommitCoordinateHelper.getToCommitSessionId();
                                if (toCommitSessionId != null) {
                                    assertThat(Integer.parseInt(toCommitSessionId))
                                            .isEqualTo(expect);
                                    expect++;
                                }
                                interruptableSleep(1000);
                            }
                        }));

        futures.add(
                executorService.submit(
                        () -> {
                            interruptableSleep(3000);
                            sessionCommitCoordinateHelper.commit(0, "1");
                            interruptableSleep(5000);
                            sessionCommitCoordinateHelper.commit(0, Constant.END_OF_SESSION);
                        }));

        futures.add(
                executorService.submit(
                        () -> {
                            interruptableSleep(2000);
                            sessionCommitCoordinateHelper.commit(1, "1");
                            interruptableSleep(5000);
                            sessionCommitCoordinateHelper.commit(1, "2");
                            interruptableSleep(1000);
                            sessionCommitCoordinateHelper.commit(1, Constant.END_OF_SESSION);
                        }));

        futures.add(
                executorService.submit(
                        () -> {
                            interruptableSleep(4000);
                            sessionCommitCoordinateHelper.commit(2, "2");
                            interruptableSleep(3000);
                            sessionCommitCoordinateHelper.commit(2, "3");
                            interruptableSleep(2000);
                            sessionCommitCoordinateHelper.commit(2, Constant.END_OF_SESSION);
                        }));

        futures.add(
                executorService.submit(
                        () -> {
                            interruptableSleep(2000);
                            sessionCommitCoordinateHelper.commit(3, "1");
                            interruptableSleep(2000);
                            sessionCommitCoordinateHelper.commit(3, "2");
                            interruptableSleep(2000);
                            sessionCommitCoordinateHelper.commit(3, "3");
                            sessionCommitCoordinateHelper.commit(3, Constant.END_OF_SESSION);
                        }));

        for (Future<?> future : futures) {
            Assertions.assertThatCode(future::get).doesNotThrowAnyException();
        }
    }

    private static void interruptableSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException("Sleep interrupted.", e);
        }
    }
}
