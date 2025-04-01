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

import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.maxcompute.common.Constant;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CommitSessionResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * The SessionCommitCoordinator class is responsible for coordinating and controlling the order of
 * session submissions by multiple concurrent executors in a distributed processing environment. It
 * ensures that: 1. Each executor must submit sessions in ascending order by session ID. 2. Each
 * executor must submit a Constant.END_OF_SESSION as a terminator after completing its session
 * submissions.
 *
 * <p>Working Principle: - Maintains an array of queues (toCommitSessionIds), with each queue
 * corresponding to an executor, to isolate the session submissions of different executors. This
 * maintains the independence and submission order of each executor. - Executors submit session IDs
 * sequentially by invoking the commit() method. The commit operation simply enqueues the session ID
 * into the corresponding executor's queue. - The getToCommitSessionId() method is tasked with
 * selecting the smallest session ID across all executors that has been "submitted" or is "no longer
 * required" for submission, allowing for further processing. "Submitted" means that the session ID
 * has been submitted by all executors; "no longer required" assumes that any subsequent session IDs
 * that are yet to be submitted will always be greater than the currently chosen ID. - Once a
 * session ID is selected by the getToCommitSessionId() method, it's removed from all executors'
 * queues, indicating that the session ID has been processed. This process ensures ordered
 * processing of the sessions and allows the system to efficiently progress. - Each processing step
 * of the session IDs is based on a key assumption: that any subsequent session ID submissions will
 * always be greater than the current processed session ID. This is guaranteed by the fact that each
 * executor commits to submitting session IDs in order and submits a special terminator
 * (Constant.END_OF_SESSION) at the end.
 *
 * <p>Note: - The class presupposes that all session IDs are comparable, and each executor strictly
 * adheres to the submission order of session IDs in ascending order. Any behavior that deviates
 * from this principle may lead to unpredictable outcomes, as it contravenes the fundamental
 * assumption of the class's design. - The introduction of Constant.END_OF_SESSION as a terminator
 * is a key aspect of this coordination strategy, as it provides a clear signal for recognizing the
 * completion status of an executor, allowing the system to determine whether all relevant sessions
 * have been processed.
 */
public class SessionCommitCoordinateHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SessionCommitCoordinateHelper.class);
    private final Queue<String>[] toCommitSessionIds;
    private final Map<String, CompletableFuture<CoordinationResponse>> toCommitFutures;
    /**
     * If any string is {@link Constant#END_OF_SESSION}, it should be considered larger than any
     * other non-{@link Constant#END_OF_SESSION} string.
     */
    private final Comparator<String> comparator =
            (String a, String b) -> {
                if (a.equals(Constant.END_OF_SESSION) || b.equals(Constant.END_OF_SESSION)) {
                    if (a.equals(b)) {
                        return 0;
                    } else if (a.equals(Constant.END_OF_SESSION)) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
                return a.compareTo(b);
            };

    private boolean isCommitting;

    public SessionCommitCoordinateHelper(int parallelism) {
        Preconditions.checkArgument(parallelism > 0);
        isCommitting = true;
        toCommitFutures = new HashMap<>();
        toCommitSessionIds = new ArrayDeque[parallelism];

        for (int i = 0; i < parallelism; i++) {
            toCommitSessionIds[i] = new ArrayDeque<>();
        }
    }

    public void clear() {
        for (Queue<String> toCommitSessionId : toCommitSessionIds) {
            toCommitSessionId.clear();
        }
        toCommitFutures.clear();
        isCommitting = true;
    }

    public CompletableFuture<CoordinationResponse> commit(int subtaskId, String sessionId) {
        LOG.info("subtask {} commit sessionId: {}", subtaskId, sessionId);
        toCommitSessionIds[subtaskId].offer(sessionId);
        if (toCommitFutures.containsKey(sessionId)) {
            return toCommitFutures.get(sessionId);
        }
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        toCommitFutures.putIfAbsent(sessionId, future);
        return future;
    }

    public String getToCommitSessionId() {
        String peekSession = null;
        for (Queue<String> commitSessionId : toCommitSessionIds) {
            if (commitSessionId.isEmpty()) {
                return null;
            }
            if (peekSession == null) {
                peekSession = commitSessionId.peek();
            } else {
                if (comparator.compare(commitSessionId.peek(), peekSession) < 0) {
                    peekSession = commitSessionId.peek();
                }
            }
        }
        // peekSession cannot be null here.
        if (peekSession.equals(Constant.END_OF_SESSION)) {
            isCommitting = false;
            return null;
        }
        for (Queue<String> toCommitSessionId : toCommitSessionIds) {
            if (toCommitSessionId.peek().equals(peekSession)) {
                toCommitSessionId.poll();
            }
        }
        return peekSession;
    }

    public void commitSuccess(String sessionId, boolean success) {
        toCommitFutures
                .get(sessionId)
                .complete(CoordinationResponseUtils.wrap(new CommitSessionResponse(success)));
    }

    public boolean isCommitting() {
        return isCommitting;
    }
}
