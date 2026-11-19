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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.DecimalPrecisionMode;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.runtime.operators.transform.async.AsyncPostTransformOperatorFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TransformTranslator}. */
class TransformTranslatorTest {

    @Test
    void testTranslateAsyncPostTransformUsesStateConsistentOperator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableId tableId = TableId.tableId("ns", "schema", "customers");
        Schema schema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        DataStream<Event> input =
                env.fromCollection(
                        Collections.singletonList((Event) new CreateTableEvent(tableId, schema)),
                        new EventTypeInfo());
        TransformDef transform =
                new TransformDef(tableId.identifier(), "*", null, null, null, null, null, null);

        DataStream<Event> result =
                new TransformTranslator()
                        .translateAsyncPostTransform(
                                input,
                                Collections.singletonList(transform),
                                "UTC",
                                DecimalPrecisionMode.UP_TO_19,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new SupportedMetadataColumn[0],
                                new OperatorUidGenerator("test"),
                                Duration.ofSeconds(30),
                                10,
                                2,
                                env);

        assertThat(result.getTransformation())
                .isInstanceOfSatisfying(
                        OneInputTransformation.class,
                        transformation ->
                                assertThat(transformation.getOperatorFactory())
                                        .isInstanceOf(AsyncPostTransformOperatorFactory.class));
    }
}
