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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.io.Serializable;

/** A {@link DataSink} for writing pipeline records to Milvus collections. */
public class MilvusDataSink implements DataSink, Serializable {

    private static final long serialVersionUID = 1L;

    private final MilvusDataSinkConfig config;

    public MilvusDataSink(MilvusDataSinkConfig config) {
        this.config = config;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new MilvusEventSink(config));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new MilvusMetadataApplier(config);
    }

    @Override
    public HashFunctionProvider<DataChangeEvent> getDataChangeEventHashFunctionProvider() {
        return new MilvusHashFunctionProvider(config);
    }
}
