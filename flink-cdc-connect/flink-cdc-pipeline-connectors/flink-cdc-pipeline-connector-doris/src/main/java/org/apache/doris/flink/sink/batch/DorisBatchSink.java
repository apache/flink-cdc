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

package org.apache.doris.flink.sink.batch;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.InitContextAdapter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;

/**
 * Copy from <a
 * href="https://github.com/apache/doris-flink-connector/blob/25.1.0/flink-doris-connector/src/main/java/org/apache/doris/flink/sink/batch/DorisBatchSink.java">DorisBatchSink</a>
 * to add {@link #createWriter(WriterInitContext)}} method.
 */
@Deprecated
@PublicEvolving
public class DorisBatchSink<IN> implements Sink<IN> {
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions dorisExecutionOptions;
    private final DorisRecordSerializer<IN> serializer;

    public DorisBatchSink(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions dorisExecutionOptions,
            DorisRecordSerializer<IN> serializer) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.dorisExecutionOptions = dorisExecutionOptions;
        this.serializer = serializer;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext initContext) throws IOException {
        DorisBatchWriter<IN> dorisBatchWriter =
                new DorisBatchWriter<IN>(
                        initContext,
                        serializer,
                        dorisOptions,
                        dorisReadOptions,
                        dorisExecutionOptions);
        return dorisBatchWriter;
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        DorisBatchWriter<IN> dorisBatchWriter =
                new DorisBatchWriter<IN>(
                        new InitContextAdapter(context),
                        serializer,
                        dorisOptions,
                        dorisReadOptions,
                        dorisExecutionOptions);
        return dorisBatchWriter;
    }

    public static <IN> DorisBatchSink.Builder<IN> builder() {
        return new DorisBatchSink.Builder<>();
    }

    /**
     * build for DorisBatchSink.
     *
     * @param <IN> record type.
     */
    public static class Builder<IN> {
        private DorisOptions dorisOptions;
        private DorisReadOptions dorisReadOptions;
        private DorisExecutionOptions dorisExecutionOptions;
        private DorisRecordSerializer<IN> serializer;

        public DorisBatchSink.Builder<IN> setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public DorisBatchSink.Builder<IN> setDorisReadOptions(DorisReadOptions dorisReadOptions) {
            this.dorisReadOptions = dorisReadOptions;
            return this;
        }

        public DorisBatchSink.Builder<IN> setDorisExecutionOptions(
                DorisExecutionOptions dorisExecutionOptions) {
            this.dorisExecutionOptions = dorisExecutionOptions;
            return this;
        }

        public DorisBatchSink.Builder<IN> setSerializer(DorisRecordSerializer<IN> serializer) {
            this.serializer = serializer;
            return this;
        }

        public DorisBatchSink<IN> build() {
            Preconditions.checkNotNull(dorisOptions);
            Preconditions.checkNotNull(dorisExecutionOptions);
            Preconditions.checkNotNull(serializer);
            if (dorisReadOptions == null) {
                dorisReadOptions = DorisReadOptions.builder().build();
            }
            return new DorisBatchSink<>(
                    dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        }
    }

    @VisibleForTesting
    public DorisReadOptions getDorisReadOptions() {
        return dorisReadOptions;
    }
}
