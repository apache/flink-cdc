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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;

/**
 * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
@Internal
public interface SourceFunction<T>
        extends org.apache.flink.streaming.api.functions.source.legacy.SourceFunction<T> {

    /**
     * Called by Flink runtime to run the source. This implementation delegates to the new
     * SourceContext version.
     */
    @Override
    default void run(
            org.apache.flink.streaming.api.functions.source.legacy.SourceFunction.SourceContext<T>
                    sourceContext)
            throws Exception {
        // Wrap the legacy SourceContext and delegate to the new run method
        run(new SourceContextWrapper<>(sourceContext));
    }

    /**
     * Run the source with the new SourceContext. Subclasses should implement this method to provide
     * source functionality.
     */
    default void run(SourceContext<T> sourceContext) throws Exception {
        // For backward compatibility: if subclass implements the legacy run method directly,
        // unwrap and call it (but this won't happen if they override the legacy method)
        throw new UnsupportedOperationException(
                "SourceFunction.run() must be implemented by subclass");
    }

    /**
     * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility
     * layer that allows Flink CDC to work across different Flink versions.
     */
    @Internal
    public interface SourceContext<T>
            extends org.apache.flink.streaming.api.functions.source.legacy.SourceFunction
                            .SourceContext<
                    T> {}

    /**
     * Wrapper that adapts a legacy SourceContext to the new SourceContext interface. This allows
     * subclasses implementing the new run(SourceContext) method to work correctly when the Flink
     * runtime calls the legacy run method.
     */
    @Internal
    class SourceContextWrapper<T> implements SourceContext<T> {
        private final org.apache.flink.streaming.api.functions.source.legacy.SourceFunction
                                .SourceContext<
                        T>
                legacyContext;

        public SourceContextWrapper(
                org.apache.flink.streaming.api.functions.source.legacy.SourceFunction.SourceContext<
                                T>
                        legacyContext) {
            this.legacyContext = legacyContext;
        }

        @Override
        public void collect(T element) {
            legacyContext.collect(element);
        }

        @Override
        public void collectWithTimestamp(T element, long timestamp) {
            legacyContext.collectWithTimestamp(element, timestamp);
        }

        @Override
        public void emitWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) {
            legacyContext.emitWatermark(mark);
        }

        @Override
        public void markAsTemporarilyIdle() {
            legacyContext.markAsTemporarilyIdle();
        }

        @Override
        public Object getCheckpointLock() {
            return legacyContext.getCheckpointLock();
        }

        @Override
        public void close() {
            legacyContext.close();
        }
    }
}
