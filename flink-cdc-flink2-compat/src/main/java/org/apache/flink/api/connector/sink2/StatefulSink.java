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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;

/**
 * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 *
 * <p>In Flink 1.x, StatefulSink was a specific interface for sinks with state. In Flink 2.x, this
 * concept is replaced by SupportsWriterState directly. This adapter provides backward compatibility
 * by implementing both the legacy and new method signatures.
 */
@Internal
public interface StatefulSink<InputT, WriterStateT>
        extends Sink<InputT>, SupportsWriterState<InputT, WriterStateT> {

    SinkWriter<InputT> createWriter(InitContext initContext) throws IOException;

    @Override
    default SinkWriter<InputT> createWriter(WriterInitContext writerInitContext)
            throws IOException {
        return createWriter(new InitContextAdapter(writerInitContext));
    }

    StatefulSinkWriter<InputT, WriterStateT> restoreWriter(
            InitContext context, Collection<WriterStateT> retrievedState) throws IOException;

    @Override
    default org.apache.flink.api.connector.sink2.StatefulSinkWriter<InputT, WriterStateT>
            restoreWriter(WriterInitContext context, Collection<WriterStateT> retrievedState)
                    throws IOException {
        return restoreWriter(new InitContextAdapter(context), retrievedState);
    }

    /** Returns the serializer for the writer state. */
    @Override
    SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer();

    /**
     * A stateful sink writer that can persist its state.
     *
     * @param <InputT> The type of the input elements
     * @param <WriterStateT> The type of the writer state
     */
    @Internal
    interface StatefulSinkWriter<InputT, WriterStateT>
            extends org.apache.flink.api.connector.sink2.StatefulSinkWriter<InputT, WriterStateT> {}
}
