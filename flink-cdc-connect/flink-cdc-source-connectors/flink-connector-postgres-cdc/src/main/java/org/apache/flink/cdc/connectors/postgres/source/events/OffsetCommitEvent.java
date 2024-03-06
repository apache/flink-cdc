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

package org.apache.flink.cdc.connectors.postgres.source.events;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator;
import org.apache.flink.cdc.connectors.postgres.source.reader.PostgresSourceReader;

/**
 * The {@link SourceEvent} that {@link PostgresSourceEnumerator} send to {@link
 * PostgresSourceReader}, which includes current assign status. PostgresSourceReader will suspend or
 * restart offset commit depends on assign status.
 */
public class OffsetCommitEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final boolean commitOffset;

    public OffsetCommitEvent(boolean commitOffset) {
        this.commitOffset = commitOffset;
    }

    public boolean isCommitOffset() {
        return commitOffset;
    }
}
