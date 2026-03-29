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

package com.oceanbase.connector.flink.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.InitContextAdapter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.oceanbase.connector.flink.ConnectorOptions;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;

import java.io.IOException;

/**
 * Copy from <a
 * href="https://github.com/oceanbase/flink-connector-oceanbase/blob/v1.2/flink-connector-oceanbase-base/src/main/java/com/oceanbase/connector/flink/sink/OceanBaseSink.java">OceanBaseSink</a>
 * to add {@link #createWriter(WriterInitContext)} method.
 */
public class OceanBaseSink<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;

    private final ConnectorOptions options;
    private final TypeSerializer<T> typeSerializer;
    private final RecordSerializationSchema<T> recordSerializer;
    private final DataChangeRecord.KeyExtractor keyExtractor;
    private final RecordFlusher recordFlusher;
    private final OceanBaseWriterEvent.Listener writerEventListener;

    public OceanBaseSink(
            ConnectorOptions options,
            TypeSerializer<T> typeSerializer,
            RecordSerializationSchema<T> recordSerializer,
            DataChangeRecord.KeyExtractor keyExtractor,
            RecordFlusher recordFlusher) {
        this(options, typeSerializer, recordSerializer, keyExtractor, recordFlusher, null);
    }

    public OceanBaseSink(
            ConnectorOptions options,
            TypeSerializer<T> typeSerializer,
            RecordSerializationSchema<T> recordSerializer,
            DataChangeRecord.KeyExtractor keyExtractor,
            RecordFlusher recordFlusher,
            OceanBaseWriterEvent.Listener writerEventListener) {
        this.options = options;
        this.typeSerializer = typeSerializer;
        this.recordSerializer = recordSerializer;
        this.keyExtractor = keyExtractor;
        this.recordFlusher = recordFlusher;
        this.writerEventListener = writerEventListener;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) {
        return new OceanBaseWriter<>(
                options,
                context,
                typeSerializer,
                recordSerializer,
                keyExtractor,
                recordFlusher,
                writerEventListener);
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext writerInitContext) throws IOException {
        return new OceanBaseWriter<>(
                options,
                new InitContextAdapter(writerInitContext),
                typeSerializer,
                recordSerializer,
                keyExtractor,
                recordFlusher,
                writerEventListener);
    }
}
