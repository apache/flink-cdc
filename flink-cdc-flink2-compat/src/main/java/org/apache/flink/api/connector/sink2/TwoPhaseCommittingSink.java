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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Internal;

/**
 * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
@Internal
public interface TwoPhaseCommittingSink<InputT, CommT>
        extends Sink<InputT>, SupportsCommitter<CommT> {

    /**
     * Compatibility adapter for PrecommittingSinkWriter.
     *
     * <p>In Flink 1.x, this was an inner interface of TwoPhaseCommittingSink. In Flink 2.x, this
     * concept is replaced by CommittingSinkWriter directly. This adapter provides backward
     * compatibility.
     */
    @Internal
    interface PrecommittingSinkWriter<InputT, CommT> extends CommittingSinkWriter<InputT, CommT> {}
}
