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

package org.apache.flink.cdc.connectors.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Compatibility adapter for Flink 1.20. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
@Internal
public class RestartStrategyUtils {

    /**
     * Disables the restart strategy for the given StreamExecutionEnvironment.
     *
     * @param env the StreamExecutionEnvironment to configure
     */
    public static void configureNoRestartStrategy(StreamExecutionEnvironment env) {
        env.configure(new Configuration().set(RestartStrategyOptions.RESTART_STRATEGY, "none"));
    }

    /**
     * Sets a fixed-delay restart strategy for the given StreamExecutionEnvironment.
     *
     * @param env the StreamExecutionEnvironment to configure
     * @param restartAttempts the number of restart attempts
     * @param delayBetweenAttempts the delay between restart attempts in milliseconds
     */
    public static void configureFixedDelayRestartStrategy(
            StreamExecutionEnvironment env, int restartAttempts, long delayBetweenAttempts) {
        configureFixedDelayRestartStrategy(
                env, restartAttempts, Duration.ofMillis(delayBetweenAttempts));
    }

    /**
     * Sets a fixed-delay restart strategy for the given StreamExecutionEnvironment.
     *
     * @param env the StreamExecutionEnvironment to configure
     * @param restartAttempts the number of restart attempts
     * @param delayBetweenAttempts the delay between restart attempts
     */
    public static void configureFixedDelayRestartStrategy(
            StreamExecutionEnvironment env, int restartAttempts, Duration delayBetweenAttempts) {
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, restartAttempts);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, delayBetweenAttempts);

        env.configure(configuration);
    }
}
