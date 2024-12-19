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

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

/**
 * Response for a {@link CommitSessionRequest}. This response is sent from {@link
 * org.apache.flink.cdc.connectors.maxcompute.coordinator.SessionManageCoordinator} to {@link
 * org.apache.flink.cdc.connectors.maxcompute.sink.MaxComputeEventWriter}.
 *
 * <p>A successful response indicates that all sessions have been committed, allowing the writer to
 * proceed to the next round of writing. Otherwise, if any session has not been successfully
 * committed, all task managers are instructed to reset to the latest checkpoint in order to retry
 * the operation.
 */
public class CommitSessionResponse implements CoordinationResponse {
    private static final long serialVersionUID = 1L;

    private final boolean success;

    public CommitSessionResponse(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }
}
