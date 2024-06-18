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

package org.apache.flink.cdc.connectors.jdbc.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.OperatorUidGenerator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.connectors.jdbc.factory.JdbcDataSinkFactory;
import org.apache.flink.cdc.connectors.jdbc.mysql.MySqlSinkTestBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

/** An abstract base class for running Jdbc sink tests. */
@ExtendWith(MiniClusterExtension.class)
public abstract class JdbcSinkTestBase {

    protected static final int DEFAULT_PARALLELISM = 1;

    protected static final StreamExecutionEnvironment ENV =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    static void before() {
        ENV.setParallelism(MySqlSinkTestBase.DEFAULT_PARALLELISM);
        ENV.enableCheckpointing(3000);
        ENV.setRestartStrategy(RestartStrategies.noRestart());
    }

    protected static DataSink createDataSink(Configuration factoryConfiguration) {
        JdbcDataSinkFactory factory = new JdbcDataSinkFactory();
        return factory.createDataSink(new MockContext(factoryConfiguration));
    }

    protected void runJobWithEvents(Configuration configuration, List<Event> events)
            throws Exception {
        DataStream<Event> stream = ENV.fromData(events, TypeInformation.of(Event.class));
        SinkDef sinkDef =
                new SinkDef(JdbcDataSinkFactory.IDENTIFIER, "JDBC MySQL Sink", configuration);
        DataSink dataSink = createDataSink(configuration);
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        SchemaChangeBehavior.EVOLVE,
                        "$$_schema_operator_$$",
                        DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT,
                        "UTC");
        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());
        OperatorUidGenerator operatorUidGenerator = new OperatorUidGenerator("jdbc_");
        stream =
                schemaOperatorTranslator.translateRegular(
                        stream,
                        MySqlSinkTestBase.DEFAULT_PARALLELISM,
                        dataSink.getMetadataApplier(),
                        new ArrayList<>());

        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                sinkDef,
                stream,
                MySqlSinkTestBase.DEFAULT_PARALLELISM,
                dataSink,
                schemaOperatorIDGenerator.generate(),
                operatorUidGenerator);
        ENV.execute("MySql Schema Evolution Test");
    }

    /** A mocked factory context. */
    protected static class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return Configuration.fromMap(Collections.singletonMap("local-time-zone", "UTC"));
        }

        @Override
        public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
        }
    }
}
