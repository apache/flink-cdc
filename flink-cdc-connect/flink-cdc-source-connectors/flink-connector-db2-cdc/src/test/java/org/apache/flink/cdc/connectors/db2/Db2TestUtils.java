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

package org.apache.flink.cdc.connectors.db2;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.util.function.SupplierWithException;

import java.util.List;
import java.util.concurrent.TimeoutException;

/** Utils to help test. */
public class Db2TestUtils {

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            Deadline timeout,
            long retryIntervalMillis,
            String errorMsg)
            throws Exception {
        while (timeout.hasTimeLeft() && !(Boolean) condition.get()) {
            long timeLeft = Math.max(0L, timeout.timeLeft().toMillis());
            Thread.sleep(Math.min(retryIntervalMillis, timeLeft));
        }

        if (!timeout.hasTimeLeft()) {
            throw new TimeoutException(errorMsg);
        }
    }

    public static void waitForJobStatus(
            JobClient client, List<JobStatus> expectedStatus, Deadline deadline) throws Exception {
        waitUntilCondition(
                () -> {
                    JobStatus currentStatus = (JobStatus) client.getJobStatus().get();
                    if (expectedStatus.contains(currentStatus)) {
                        return true;
                    } else if (currentStatus.isTerminalState()) {
                        try {
                            client.getJobExecutionResult().get();
                        } catch (Exception e) {
                            throw new IllegalStateException(
                                    String.format(
                                            "Job has entered %s state, but expecting %s",
                                            currentStatus, expectedStatus),
                                    e);
                        }

                        throw new IllegalStateException(
                                String.format(
                                        "Job has entered a terminal state %s, but expecting %s",
                                        currentStatus, expectedStatus));
                    } else {
                        return false;
                    }
                },
                deadline,
                100L,
                "Condition was not met in given timeout.");
    }
}
