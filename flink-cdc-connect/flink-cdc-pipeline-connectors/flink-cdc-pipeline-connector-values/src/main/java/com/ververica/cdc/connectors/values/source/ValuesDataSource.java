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

package com.ververica.cdc.connectors.values.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.TableException;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.common.source.EventSourceProvider;
import com.ververica.cdc.common.source.FlinkSourceProvider;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.connectors.values.ValuesDatabase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/** A {@link DataSource} for "values" connector that supports schema evolution. */
@Internal
public class ValuesDataSource implements DataSource {

    /** index of testCase for {@link ValuesDataSourceHelper}. */
    private final ValuesDataSourceHelper.EventSetId eventSetId;

    /** index for {@link EventIteratorReader} to fail when reading. */
    private final int failAtPos;

    public ValuesDataSource(ValuesDataSourceHelper.EventSetId eventSetId) {
        this.eventSetId = eventSetId;
        this.failAtPos = Integer.MAX_VALUE;
    }

    public ValuesDataSource(ValuesDataSourceHelper.EventSetId eventSetId, int failAtPos) {
        this.eventSetId = eventSetId;
        this.failAtPos = failAtPos;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        ValuesDataSourceHelper.setSourceEvents(eventSetId);
        HybridSource<Event> hybridSource =
                HybridSource.builder(new ValuesSource(failAtPos, eventSetId, true))
                        .addSource(new ValuesSource(failAtPos, eventSetId, false))
                        .build();
        return FlinkSourceProvider.of(hybridSource);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new ValuesDatabase.ValuesMetadataAccessor();
    }

    /**
     * A flink {@link Source} implementation for end to end tests, splits are created by {@link
     * ValuesDataSourceHelper}.
     */
    private static class ValuesSource
            implements Source<Event, EventIteratorSplit, Collection<EventIteratorSplit>> {

        private static final long serialVersionUID = 1L;

        private final int failAtPos;

        private final ValuesDataSourceHelper.EventSetId eventSetId;

        /** True this source is in snapshot stage, otherwise is in incremental stage. */
        private final boolean isInSnapshotPhase;

        public ValuesSource(
                int failAtPos,
                ValuesDataSourceHelper.EventSetId eventSetId,
                boolean isInSnapshotPhase) {
            this.failAtPos = failAtPos;
            this.eventSetId = eventSetId;
            this.isInSnapshotPhase = isInSnapshotPhase;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SplitEnumerator<EventIteratorSplit, Collection<EventIteratorSplit>> createEnumerator(
                SplitEnumeratorContext<EventIteratorSplit> enumContext) {
            ValuesDataSourceHelper.setSourceEvents(eventSetId);
            Collection<EventIteratorSplit> eventIteratorSplits = new ArrayList<>();
            List<List<Event>> eventWithSplits = ValuesDataSourceHelper.getSourceEvents();
            // make the last EventIteratorSplit of eventWithSplits to be an incremental
            // EventIteratorSplit.
            if (isInSnapshotPhase) {
                for (int i = 0; i < eventWithSplits.size() - 1; i++) {
                    eventIteratorSplits.add(new EventIteratorSplit(i, 0));
                }
            } else {
                eventIteratorSplits.add(new EventIteratorSplit(eventWithSplits.size() - 1, 0));
            }
            return new IteratorSourceEnumerator<>(enumContext, eventIteratorSplits);
        }

        @Override
        public SplitEnumerator<EventIteratorSplit, Collection<EventIteratorSplit>>
                restoreEnumerator(
                        SplitEnumeratorContext<EventIteratorSplit> enumContext,
                        Collection<EventIteratorSplit> checkpoint) {
            return new IteratorSourceEnumerator<>(enumContext, checkpoint);
        }

        @Override
        public SimpleVersionedSerializer<EventIteratorSplit> getSplitSerializer() {
            return new EventIteratorSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Collection<EventIteratorSplit>>
                getEnumeratorCheckpointSerializer() {
            return new EventIteratorEnumeratorSerializer();
        }

        @Override
        public SourceReader<Event, EventIteratorSplit> createReader(
                SourceReaderContext readerContext) {
            return new EventIteratorReader(readerContext, failAtPos, eventSetId);
        }

        private static void serializeEventIteratorSplit(
                DataOutputViewStreamWrapper view, EventIteratorSplit split) throws IOException {
            view.writeInt(split.splitId);
            view.writeInt(split.pos);
        }

        private static EventIteratorSplit deserializeEventIteratorSplit(
                DataInputViewStreamWrapper view) throws IOException {
            int splitId = view.readInt();
            int pos = view.readInt();
            return new EventIteratorSplit(splitId, pos);
        }

        /** A serializer for {@link EventIteratorSplit}, use in {@link ValuesSource}. */
        private static class EventIteratorSplitSerializer
                implements SimpleVersionedSerializer<EventIteratorSplit> {

            private static final int SPLIT_VERSION = 1;

            @Override
            public int getVersion() {
                return SPLIT_VERSION;
            }

            @Override
            public byte[] serialize(EventIteratorSplit split) throws IOException {
                try (ByteArrayOutputStream bao = new ByteArrayOutputStream(256)) {
                    DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(bao);
                    serializeEventIteratorSplit(view, split);
                    return bao.toByteArray();
                }
            }

            @Override
            public EventIteratorSplit deserialize(int version, byte[] serialized)
                    throws IOException {
                if (version != SPLIT_VERSION) {
                    throw new TableException(
                            String.format(
                                    "Can't serialized data with version %d because the serializer version is %d.",
                                    version, SPLIT_VERSION));
                }

                try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized)) {
                    DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bis);
                    return deserializeEventIteratorSplit(view);
                }
            }
        }

        /**
         * A serializer for a collection of {@link EventIteratorSplit}, use in {@link ValuesSource}.
         */
        private static class EventIteratorEnumeratorSerializer
                implements SimpleVersionedSerializer<Collection<EventIteratorSplit>> {

            private static final int ENUMERATOR_VERSION = 1;

            @Override
            public int getVersion() {
                return ENUMERATOR_VERSION;
            }

            @Override
            public byte[] serialize(Collection<EventIteratorSplit> splits) throws IOException {
                try (ByteArrayOutputStream bao = new ByteArrayOutputStream(256);
                        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(bao)) {
                    view.writeInt(splits.size());
                    for (EventIteratorSplit split : splits) {
                        serializeEventIteratorSplit(view, split);
                    }
                    return bao.toByteArray();
                }
            }

            @Override
            public Collection<EventIteratorSplit> deserialize(int version, byte[] serialized)
                    throws IOException {
                try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bis)) {
                    List<EventIteratorSplit> splits = new ArrayList<>();
                    int size = view.readInt();
                    for (int i = 0; i < size; i++) {
                        splits.add(deserializeEventIteratorSplit(view));
                    }
                    return splits;
                }
            }
        }
    }

    /** A sourceReader to emit {@link Event} and can fail at specific position. */
    private static class EventIteratorReader
            extends IteratorSourceReader<Event, Iterator<Event>, EventIteratorSplit> {

        private static volatile boolean checkpointed = false;

        private static volatile boolean failedBefore = false;

        // position for this Split to fail
        private final int failAtPos;

        private final ValuesDataSourceHelper.EventSetId eventSetId;

        private int numberOfEventsEmit = 0;

        public EventIteratorReader(
                SourceReaderContext context,
                int failAtPos,
                ValuesDataSourceHelper.EventSetId eventSetId) {
            super(context);
            this.failAtPos = failAtPos;
            this.eventSetId = eventSetId;
        }

        @Override
        public void start() {
            ValuesDataSourceHelper.setSourceEvents(eventSetId);
            super.start();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Event> output) {
            if (!failedBefore && numberOfEventsEmit >= failAtPos) {
                if (!checkpointed) {
                    // Slow down to wait for checkpointing.
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                        throw new TableException("Failed to wait checkpoint finishes.", e);
                    }
                    return InputStatus.MORE_AVAILABLE;
                } else {
                    failedBefore = true;
                    throw new RuntimeException("Artificial Exception.");
                }
            }
            numberOfEventsEmit++;
            return super.pollNext(output);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            super.notifyCheckpointComplete(checkpointId);
            checkpointed = true;
        }
    }

    /**
     * A IteratorSourceSplit to produce {@link ChangeEvent}, based on the events provided by {@link
     * ValuesDataSourceHelper}.
     */
    private static class EventIteratorSplit implements IteratorSourceSplit<Event, Iterator<Event>> {

        // event pass to collector
        private final List<Event> events;

        private final int splitId;

        private int pos;

        public EventIteratorSplit(int splitId, int pos) {
            this.splitId = splitId;
            this.pos = pos;
            List<Event> eventOfSplit = ValuesDataSourceHelper.getSourceEvents().get(splitId);
            events = eventOfSplit.subList(pos, eventOfSplit.size());
        }

        @Override
        public Iterator<Event> getIterator() {

            Iterator<Event> inner = events.iterator();

            return new Iterator<Event>() {
                @Override
                public boolean hasNext() {
                    return inner.hasNext();
                }

                @Override
                public Event next() {
                    pos++;
                    return inner.next();
                }
            };
        }

        @Override
        public IteratorSourceSplit<Event, Iterator<Event>> getUpdatedSplitForIterator(
                Iterator<Event> iterator) {
            return new EventIteratorSplit(splitId, pos);
        }

        @Override
        public String splitId() {
            return "split_" + splitId;
        }
    }
}
