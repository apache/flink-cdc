/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.Options;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** A {@link Committer} to commit write results for multiple tables. */
public class PaimonCommitter implements Committer<MultiTableCommittable> {

    private final StoreMultiCommitter storeMultiCommitter;

    public PaimonCommitter(Options catalogOptions, String commitUser) {
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        // flinkMetricGroup could be passed after FLIP-371.
        storeMultiCommitter = new StoreMultiCommitter(() -> catalog, commitUser, null);
    }

    @Override
    public void commit(Collection<CommitRequest<MultiTableCommittable>> commitRequests)
            throws IOException, InterruptedException {
        if (commitRequests.isEmpty()) {
            return;
        }
        List<MultiTableCommittable> committables =
                commitRequests.stream()
                        .map(CommitRequest::getCommittable)
                        .collect(Collectors.toList());
        long checkpointId = committables.get(0).checkpointId();
        WrappedManifestCommittable wrappedManifestCommittable =
                storeMultiCommitter.combine(checkpointId, 1L, committables);
        storeMultiCommitter.commit(Collections.singletonList(wrappedManifestCommittable));
    }

    @Override
    public void close() throws Exception {
        storeMultiCommitter.close();
    }
}
