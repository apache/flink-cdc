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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.TableChange;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Preserves native monitor state and closes the catalog used by its reader. */
final class ClosingMonitorSource<SplitT extends SourceSplit, CheckpointT>
        implements Source<TableChange, SplitT, CheckpointT> {
    private static final long serialVersionUID = 1L;
    private final Source<TableChange, SplitT, CheckpointT> delegate;
    private final TableLoader loader;

    ClosingMonitorSource(Source<TableChange, SplitT, CheckpointT> delegate, TableLoader loader) {
        this.delegate = delegate;
        this.loader = loader;
    }

    @Override
    public Boundedness getBoundedness() {
        return delegate.getBoundedness();
    }

    @Override
    public SourceReader<TableChange, SplitT> createReader(SourceReaderContext context)
            throws Exception {
        SourceReader<TableChange, SplitT> reader;
        try {
            reader = delegate.createReader(context);
        } catch (Exception failure) {
            try {
                loader.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
        return new SourceReader<TableChange, SplitT>() {
            @Override
            public void start() {
                reader.start();
            }

            @Override
            public InputStatus pollNext(ReaderOutput<TableChange> output) throws Exception {
                return reader.pollNext(output);
            }

            @Override
            public List<SplitT> snapshotState(long checkpointId) {
                return reader.snapshotState(checkpointId);
            }

            @Override
            public CompletableFuture<Void> isAvailable() {
                return reader.isAvailable();
            }

            @Override
            public void addSplits(List<SplitT> splits) {
                reader.addSplits(splits);
            }

            @Override
            public void notifyNoMoreSplits() {
                reader.notifyNoMoreSplits();
            }

            @Override
            public void handleSourceEvents(SourceEvent event) {
                reader.handleSourceEvents(event);
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) throws Exception {
                reader.notifyCheckpointComplete(checkpointId);
            }

            @Override
            public void notifyCheckpointAborted(long checkpointId) throws Exception {
                reader.notifyCheckpointAborted(checkpointId);
            }

            @Override
            public void pauseOrResumeSplits(Collection<String> pause, Collection<String> resume) {
                reader.pauseOrResumeSplits(pause, resume);
            }

            @Override
            public void close() throws Exception {
                try (TableLoader ignored = loader) {
                    reader.close();
                }
            }
        };
    }

    // Native enumeration and deserialization only create deferred table facades. Catalog access
    // happens when the reader polls; keep both serializers unchanged for checkpoint compatibility.
    @Override
    public SplitEnumerator<SplitT, CheckpointT> createEnumerator(
            SplitEnumeratorContext<SplitT> context) throws Exception {
        return delegate.createEnumerator(context);
    }

    @Override
    public SplitEnumerator<SplitT, CheckpointT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> context, CheckpointT checkpoint) throws Exception {
        return delegate.restoreEnumerator(context, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
        return delegate.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<CheckpointT> getEnumeratorCheckpointSerializer() {
        return delegate.getEnumeratorCheckpointSerializer();
    }
}
