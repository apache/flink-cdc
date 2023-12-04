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

package com.ververica.cdc.connectors.doris.sink;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.FlinkSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;
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
                            new DorisEventSerializer(zoneId)));
        } else {
            return FlinkSinkProvider.of(
                    new DorisBatchSink<>(
                            dorisOptions,
                            readOptions,
                            executionOptions,
                            new DorisEventSerializer(zoneId)));
        }
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new DorisMetadataApplier(dorisOptions, configuration);
    }
}
