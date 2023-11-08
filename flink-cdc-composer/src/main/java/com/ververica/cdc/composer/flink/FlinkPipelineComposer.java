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

package com.ververica.cdc.composer.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.composer.PipelineComposer;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;

import java.nio.file.Path;
import java.util.List;

/** Composer for translating data pipeline to a Flink DataStream job. */
public class FlinkPipelineComposer implements PipelineComposer {

    private final StreamExecutionEnvironment env;
    private final boolean isBlocking;

    public static FlinkPipelineComposer ofRemoteCluster(
            String host, int port, List<Path> additionalJars) {
        String[] jarPaths = additionalJars.stream().map(Path::toString).toArray(String[]::new);
        return new FlinkPipelineComposer(
                StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarPaths), false);
    }

    public static FlinkPipelineComposer ofMiniCluster() {
        return new FlinkPipelineComposer(StreamExecutionEnvironment.createLocalEnvironment(), true);
    }

    private FlinkPipelineComposer(StreamExecutionEnvironment env, boolean isBlocking) {
        this.env = env;
        this.isBlocking = isBlocking;
    }

    @Override
    public PipelineExecution compose(PipelineDef pipelineDef) {
        return new FlinkPipelineExecution(env, "CDC Job", isBlocking);
    }
}
