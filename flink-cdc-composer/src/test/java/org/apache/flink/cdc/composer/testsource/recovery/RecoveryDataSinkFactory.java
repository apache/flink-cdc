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

package org.apache.flink.cdc.composer.testsource.recovery;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.sink.DataSink;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.util.Set;

/** Factory for {@link RecoveryDataSink}. */
public class RecoveryDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "recovery-test-sink";

    @Override
    public DataSink createDataSink(Context context) {
        return new RecoveryDataSink();
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return ImmutableSet.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return ImmutableSet.of();
    }
}
