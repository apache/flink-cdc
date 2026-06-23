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

package org.apache.flink.cdc.connectors.db2.source.reader;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.db2.source.Db2EventDeserializer;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfigFactory;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.offset.LsnFactory;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link Db2PipelineRecordEmitter}. */
class Db2PipelineRecordEmitterTest {

    @Test
    void constructorDoesNotFetchAllTableSchemas() {
        Db2SourceConfigFactory configFactory = new Db2SourceConfigFactory();
        configFactory
                .hostname("localhost")
                .port(1)
                .databaseList("TESTDB")
                .tableList("DB2INST1.PRODUCTS")
                .username("db2inst1")
                .password("password")
                .startupOptions(StartupOptions.initial())
                .connectTimeout(Duration.ofMillis(1))
                .connectMaxRetries(0);
        Db2SourceConfig sourceConfig = configFactory.create(0);

        assertThatCode(
                        () ->
                                new Db2PipelineRecordEmitter<>(
                                        new Db2EventDeserializer(DebeziumChangelogMode.ALL, true),
                                        null,
                                        sourceConfig,
                                        new LsnFactory(),
                                        new Db2Dialect(sourceConfig)))
                .doesNotThrowAnyException();
    }
}
