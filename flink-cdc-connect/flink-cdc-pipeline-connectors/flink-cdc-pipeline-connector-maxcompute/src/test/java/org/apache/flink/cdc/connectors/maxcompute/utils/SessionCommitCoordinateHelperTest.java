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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** */
public class SessionCommitCoordinateHelperTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {
        SessionCommitCoordinateHelper sessionCommitCoordinateHelper =
                new SessionCommitCoordinateHelper(4);
        sessionCommitCoordinateHelper.clear();
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        Future<?> future =
                executorService.submit(
                        () -> {
                            int expect = 1;
                            while (sessionCommitCoordinateHelper.isCommitting()) {
                                try {
                                    String toCommitSessionId =
                                            sessionCommitCoordinateHelper.getToCommitSessionId();
                                    if (toCommitSessionId != null) {
                                        Assert.assertEquals(
                                                expect, Integer.parseInt(toCommitSessionId));
                                        expect++;
                                    }
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        });

        executorService.submit(
                () -> {
                    try {
                        Thread.sleep(3000);
                        sessionCommitCoordinateHelper.commit(0, "1");
                        Thread.sleep(5000);
                        sessionCommitCoordinateHelper.commit(0, Constant.END_OF_SESSION);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        executorService.submit(
                () -> {
                    try {
                        Thread.sleep(2000);
                        sessionCommitCoordinateHelper.commit(1, "1");
                        Thread.sleep(5000);
                        sessionCommitCoordinateHelper.commit(1, "2");
                        Thread.sleep(1000);
                        sessionCommitCoordinateHelper.commit(1, Constant.END_OF_SESSION);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        executorService.submit(
                () -> {
                    try {
                        Thread.sleep(4000);
                        sessionCommitCoordinateHelper.commit(2, "2");
                        Thread.sleep(3000);
                        sessionCommitCoordinateHelper.commit(2, "3");
                        Thread.sleep(2000);
                        sessionCommitCoordinateHelper.commit(2, Constant.END_OF_SESSION);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        executorService.submit(
                () -> {
                    try {
                        Thread.sleep(2000);
                        sessionCommitCoordinateHelper.commit(3, "1");
                        Thread.sleep(2000);
                        sessionCommitCoordinateHelper.commit(3, "2");
                        Thread.sleep(2000);
                        sessionCommitCoordinateHelper.commit(3, "3");
                        sessionCommitCoordinateHelper.commit(3, Constant.END_OF_SESSION);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

        future.get();
    }
}
