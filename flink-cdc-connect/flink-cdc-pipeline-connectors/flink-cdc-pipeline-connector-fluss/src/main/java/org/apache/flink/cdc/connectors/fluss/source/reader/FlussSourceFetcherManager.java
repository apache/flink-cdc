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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.source.SingleThreadFetcherManagerAdapter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.fluss.metadata.TablePath;

import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;

/** Executes table removal on the connector's single fetcher thread. */
class FlussSourceFetcherManager
        extends SingleThreadFetcherManagerAdapter<FlussSourceRecord, FlussSplitBase> {

    FlussSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> elementsQueue,
            Supplier<FlussSplitReader> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier::get);
    }

    void removeTables(Set<TablePath> tablePaths) {
        if (tablePaths.isEmpty()) {
            return;
        }
        SplitFetcher<FlussSourceRecord, FlussSplitBase> fetcher = getRunningFetcher();
        if (fetcher == null) {
            fetcher = createSplitFetcher();
            enqueueRemovalTask(fetcher, tablePaths);
            startFetcher(fetcher);
        } else {
            enqueueRemovalTask(fetcher, tablePaths);
            ((FlussSplitReader) fetcher.getSplitReader()).wakeUp();
        }
    }

    private void enqueueRemovalTask(
            SplitFetcher<FlussSourceRecord, FlussSplitBase> fetcher, Set<TablePath> tablePaths) {
        FlussSplitReader splitReader = (FlussSplitReader) fetcher.getSplitReader();
        fetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() throws IOException {
                        splitReader.removeTables(tablePaths);
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
