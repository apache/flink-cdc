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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;

import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** A {@link Committer} to commit write results for multiple tables. */
public class PaimonCommitter implements Committer<MultiTableCommittable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonCommitter.class);

    private final StoreMultiCommitter storeMultiCommitter;

    public PaimonCommitter(Options catalogOptions, String commitUser) {
        // flinkMetricGroup could be passed after FLIP-371.
        storeMultiCommitter =
                new StoreMultiCommitter(
                        () -> FlinkCatalogFactory.createPaimonCatalog(catalogOptions),
                        org.apache.paimon.flink.sink.Committer.createContext(
                                commitUser, null, true, false, null, 1, 1));
    }

    @Override
    public void commit(Collection<CommitRequest<MultiTableCommittable>> commitRequests)
            throws IOException {
        if (commitRequests.isEmpty()) {
            return;
        }

        List<MultiTableCommittable> committables =
                commitRequests.stream()
                        .map(CommitRequest::getCommittable)
                        .collect(Collectors.toList());
        // All CommitRequest shared the same checkpointId.
        long checkpointId = committables.get(0).checkpointId();
        WrappedManifestCommittable wrappedManifestCommittable =
                storeMultiCommitter.combine(checkpointId, 1L, committables);
        try {
            storeMultiCommitter.filterAndCommit(
                    Collections.singletonList(wrappedManifestCommittable), true, false);
            commitRequests.forEach(CommitRequest::signalAlreadyCommitted);
            LOGGER.info(
                    "Commit succeeded for {} with {} committable",
                    checkpointId,
                    committables.size());
        } catch (Exception e) {
            commitRequests.forEach(CommitRequest::retryLater);
            LOGGER.warn(
                    "Commit failed for {} with {} committable",
                    checkpointId,
                    committables.size(),
                    e);
        }
    }

    @Override
    public void close() throws Exception {
        storeMultiCommitter.close();
    }
}
