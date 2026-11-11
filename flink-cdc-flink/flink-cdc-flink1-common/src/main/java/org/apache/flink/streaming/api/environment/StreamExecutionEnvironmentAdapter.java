/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Compatibility adapter for the Flink 1.20 line: gives flink-cdc main code read access to the
 * protected {@code StreamExecutionEnvironment#configuration} field without reflection, and routes
 * version-sensitive source-adding calls (the {@code SourceFunction} parameter type moves to the
 * {@code legacy} subpackage on Flink 2.x). This class must stay in the same package as {@code
 * StreamExecutionEnvironment}; each Flink generation carries its own copy in its common module.
 */
@Internal
public class StreamExecutionEnvironmentAdapter {

    private StreamExecutionEnvironmentAdapter() {}

    /** Returns the environment's mutable configuration. */
    public static Configuration getConfiguration(StreamExecutionEnvironment environment) {
        return environment.configuration;
    }

    /** Adds a legacy {@code SourceFunction} source, isolating its generational FQCN drift. */
    public static <OUT> DataStreamSource<OUT> addSource(
            StreamExecutionEnvironment environment,
            SourceFunction<OUT> sourceFunction,
            TypeInformation<OUT> typeInfo) {
        return environment.addSource(sourceFunction, typeInfo);
    }
}
