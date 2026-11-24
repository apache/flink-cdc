/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;

import java.io.Serializable;
import java.util.Collection;

/**
 * Compatibility adapter for Flink 1.20. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
public abstract class AsyncSinkWriterAdapter<InputT, RequestEntryT extends Serializable>
        extends AsyncSinkWriter<InputT, RequestEntryT> {

    public AsyncSinkWriterAdapter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            Sink.InitContext context,
            AsyncSinkWriterConfiguration configuration,
            Collection<BufferedRequestState<RequestEntryT>> bufferedRequestStates) {
        super(elementConverter, context, configuration, bufferedRequestStates);
    }
}
