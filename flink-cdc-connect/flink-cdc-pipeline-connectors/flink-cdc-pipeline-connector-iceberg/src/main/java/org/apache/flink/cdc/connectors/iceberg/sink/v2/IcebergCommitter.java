/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

/**
 * This class implements the Flink SinkV2 {@link Committer} interface to implement the Iceberg
 * commits. The implementation builds on the following assumptions:
 *
 * <ul>
 *   <li>There is a single {@link org.apache.iceberg.flink.sink.IcebergCommittable} for every
 *       checkpoint
 *   <li>There is no late checkpoint - if checkpoint 'x' has received in one call, then after a
 *       successful run only checkpoints &gt; x will arrive
 *   <li>There is no other writer which would generate another commit to the same branch with the
 *       same jobId-operatorId-checkpointId triplet
 * </ul>
 */
public class IcebergCommitter implements Committer<IcebergCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitter.class);

    public IcebergCommitter(Map<String, String> catalogOptions, String commitUser) {}

    @Override
    public void commit(Collection<CommitRequest<IcebergCommittable>> commitRequests)
            throws IOException, InterruptedException {
        if (commitRequests.isEmpty()) {
            return;
        }
        NavigableMap<Long, CommitRequest<IcebergCommittable>> commitRequestMap = Maps.newTreeMap();
        for (CommitRequest<IcebergCommittable> request : commitRequests) {
            commitRequestMap.put(request.getCommittable().checkpointId(), request);
        }
        // TODO commit
        List<IcebergCommittable> committables =
                commitRequests.stream()
                        .map(CommitRequest::getCommittable)
                        .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {}
}
