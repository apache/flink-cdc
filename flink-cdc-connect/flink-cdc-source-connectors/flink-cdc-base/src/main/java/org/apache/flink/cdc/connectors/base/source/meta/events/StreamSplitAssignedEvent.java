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

package org.apache.flink.cdc.connectors.base.source.meta.events;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReader;

import java.util.Objects;

/**
 * The {@link SourceEvent} that {@link IncrementalSourceReader} sends to {@link
 * IncrementalSourceEnumerator} to notify the {@link StreamSplit} assigned to itself.
 */
public class StreamSplitAssignedEvent implements SourceEvent {

    private static final long serialVersionUID = 2L;

    private final StreamSplit streamSplit;

    public StreamSplitAssignedEvent(StreamSplit streamSplit) {
        this.streamSplit = streamSplit;
    }

    public StreamSplit getStreamSplit() {
        return streamSplit;
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof StreamSplitAssignedEvent)) {
            return false;
        }

        StreamSplitAssignedEvent that = (StreamSplitAssignedEvent) o;
        return Objects.equals(streamSplit, that.streamSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamSplit);
    }

    @Override
    public String toString() {
        return "StreamSplitAssignedEvent{" + "streamSplit=" + streamSplit + '}';
    }
}
