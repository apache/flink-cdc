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

package org.apache.flink.cdc.connectors.maxcompute.coordinator.message;

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

/**
 * The commit session request from {@link
 * org.apache.flink.cdc.connectors.maxcompute.sink.MaxComputeEventWriter} to {@link
 * org.apache.flink.cdc.connectors.maxcompute.coordinator.SessionManageCoordinator}. Which is a type
 * of {@link SyncRequest}.
 */
public class CommitSessionRequest implements CoordinationRequest {
    private static final long serialVersionUID = 1L;

    private final int operatorIndex;
    private final String sessionId;

    public CommitSessionRequest(int operatorIndex, String sessionId) {
        this.operatorIndex = operatorIndex;
        this.sessionId = sessionId;
    }

    public int getOperatorIndex() {
        return operatorIndex;
    }

    public String getSessionId() {
        return sessionId;
    }

    @Override
    public String toString() {
        return "CommitSessionRequest{"
                + "operatorIndex="
                + operatorIndex
                + ", sessionId='"
                + sessionId
                + '\''
                + '}';
    }
}
