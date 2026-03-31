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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaColumnCaseFormat;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TransformTranslator} post-transform wiring (including implicit case-format). */
class TransformTranslatorTest {

    @Test
    void testImplicitPostTransformAddsGraphTransformationWhenNoTransformsAndUpperCaseFormat() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Event> events = Lists.newArrayList(new EmptyEvent());
        DataStream<Event> input = env.fromCollection(events);
        int afterSource = env.getTransformations().size();

        new TransformTranslator()
                .translatePostTransform(
                        input,
                        Collections.emptyList(),
                        "systemDefault",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0],
                        SchemaColumnCaseFormat.UPPER,
                        new OperatorUidGenerator());

        assertThat(env.getTransformations().size()).isEqualTo(afterSource + 1);
    }

    @Test
    void testNoExtraPostTransformWhenAsIsAndNoTransforms() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Event> events = Lists.newArrayList(new EmptyEvent());
        DataStream<Event> input = env.fromCollection(events);
        int afterSource = env.getTransformations().size();

        new TransformTranslator()
                .translatePostTransform(
                        input,
                        Collections.emptyList(),
                        "systemDefault",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0],
                        SchemaColumnCaseFormat.AS_IS,
                        new OperatorUidGenerator());

        assertThat(env.getTransformations().size()).isEqualTo(afterSource);
    }

    @Test
    void testTransformWithOnlyColumnCaseFormatRegistersPostTransform() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Event> events = Lists.newArrayList(new EmptyEvent());
        DataStream<Event> input = env.fromCollection(events);
        int afterSource = env.getTransformations().size();

        new TransformTranslator()
                .translatePostTransform(
                        input,
                        Collections.singletonList(
                                new TransformDef(
                                        "db.t",
                                        null,
                                        null,
                                        "",
                                        "",
                                        "",
                                        ",",
                                        "",
                                        "",
                                        SchemaColumnCaseFormat.UPPER)),
                        "systemDefault",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new SupportedMetadataColumn[0],
                        SchemaColumnCaseFormat.AS_IS,
                        new OperatorUidGenerator());

        assertThat(env.getTransformations().size()).isEqualTo(afterSource + 1);
    }

    private static class EmptyEvent implements Event {}
}
