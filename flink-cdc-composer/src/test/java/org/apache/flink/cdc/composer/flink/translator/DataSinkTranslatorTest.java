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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

/** A test for {@link DataSinkTranslator}. */
class DataSinkTranslatorTest {

    @Test
    void testPreWriteWithoutCommitSink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Event> mockEvents = Lists.newArrayList(new EmptyEvent(), new EmptyEvent());
        DataStreamSource<Event> inputStream = env.fromCollection(mockEvents);
        DataSinkTranslator translator = new DataSinkTranslator();

        // Node hash must be a 32 character String that describes a hex code
        String uid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        MockPreWriteWithoutCommitSink mockPreWriteWithoutCommitSink =
                new MockPreWriteWithoutCommitSink(uid);
        translator.sinkTo(
                inputStream,
                mockPreWriteWithoutCommitSink,
                "testPreWriteWithoutCommitSink",
                false,
                new OperatorID(),
                new OperatorUidGenerator());

        // Check if the `addPreWriteTopology` is called, and the uid is set when the transformation
        // added
        OneInputTransformation<Event, Event> oneInputTransformation =
                (OneInputTransformation) env.getTransformations().get(0);
        Transformation<?> reblanceTransformation = oneInputTransformation.getInputs().get(0);
        Assertions.assertThat(reblanceTransformation.getUserProvidedNodeHash()).isEqualTo(uid);
    }

    private static class EmptyEvent implements Event {}

    private static class MockPreWriteWithoutCommitSink implements WithPreWriteTopology<Event> {

        private final String uid;

        public MockPreWriteWithoutCommitSink(String uid) {
            this.uid = uid;
        }

        @Override
        public DataStream<Event> addPreWriteTopology(DataStream<Event> inputDataStream) {
            // return a new DataSteam with specified uid
            DataStream<Event> rebalance = inputDataStream.rebalance();
            rebalance.getTransformation().setUidHash(uid);
            return rebalance;
        }

        @Override
        public SinkWriter<Event> createWriter(InitContext context) throws IOException {
            return null;
        }
    }
}
