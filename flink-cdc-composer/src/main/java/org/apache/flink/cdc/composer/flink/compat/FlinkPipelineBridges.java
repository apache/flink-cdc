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

package org.apache.flink.cdc.composer.flink.compat;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.flink.compat.FlinkPipelineBridge;

import java.util.Iterator;
import java.util.ServiceLoader;

/** Loads the {@link FlinkPipelineBridge} implementation via {@link ServiceLoader}. */
@Internal
public final class FlinkPipelineBridges {

    /**
     * Returns the first available {@link FlinkPipelineBridge} implementation.
     *
     * <p>Exactly one compat JAR (flink-cdc-flink-compat-flink1 or flink-cdc-flink-compat-flink2)
     * must be on the classpath and provide a service implementation.
     */
    public static FlinkPipelineBridge getDefault() {
        ServiceLoader<FlinkPipelineBridge> loader =
                ServiceLoader.load(
                        FlinkPipelineBridge.class, FlinkPipelineBridge.class.getClassLoader());
        Iterator<FlinkPipelineBridge> it = loader.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException(
                    "No FlinkPipelineBridge implementation found. Add flink-cdc-flink-compat-flink1 "
                            + "or flink-cdc-flink-compat-flink2 to the classpath.");
        }
        FlinkPipelineBridge bridge = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException(
                    "Multiple FlinkPipelineBridge implementations found. Only one compat JAR "
                            + "(flink-cdc-flink-compat-flink1 or flink-cdc-flink-compat-flink2) should be on the classpath.");
        }
        return bridge;
    }

    private FlinkPipelineBridges() {}
}
