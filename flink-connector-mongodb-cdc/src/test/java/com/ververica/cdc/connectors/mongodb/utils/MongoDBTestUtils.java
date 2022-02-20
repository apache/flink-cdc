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

package com.ververica.cdc.connectors.mongodb.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    public static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    /** The type of failover. */
    public enum FailoverType {
        TM,
        JM,
        NONE
    }

    /** The phase of failover. */
    public enum FailoverPhase {
        SNAPSHOT,
        STREAM,
        NEVER
    }

    public static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    public static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }
}
