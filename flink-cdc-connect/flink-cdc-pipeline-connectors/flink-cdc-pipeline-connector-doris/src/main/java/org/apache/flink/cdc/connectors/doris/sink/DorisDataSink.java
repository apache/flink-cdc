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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.batch.DorisBatchSink;

import java.io.Serializable;
import java.time.ZoneId;

/** A {@link DataSink} for "Doris" connector. */
public class DorisDataSink implements DataSink, Serializable {

    private final DorisOptions dorisOptions;
    private final DorisReadOptions readOptions;
    private final DorisExecutionOptions executionOptions;
    private Configuration configuration;
    private final ZoneId zoneId;

    public DorisDataSink(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions dorisExecutionOptions,
            Configuration configuration,
            ZoneId zoneId) {
        this.dorisOptions = dorisOptions;
        this.readOptions = dorisReadOptions;
        this.executionOptions = dorisExecutionOptions;
        this.configuration = configuration;
        this.zoneId = zoneId;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        if (!executionOptions.enableBatchMode()) {
            return FlinkSinkProvider.of(
                    new DorisSink<>(
                            dorisOptions,
                            readOptions,
                            executionOptions,
                            new DorisEventSerializer(zoneId, configuration)));
        } else {
            return FlinkSinkProvider.of(
                    new DorisBatchSink<>(
                            dorisOptions,
                            readOptions,
                            executionOptions,
                            new DorisEventSerializer(zoneId, configuration)));
        }
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new DorisMetadataApplier(dorisOptions, configuration);
    }
}
