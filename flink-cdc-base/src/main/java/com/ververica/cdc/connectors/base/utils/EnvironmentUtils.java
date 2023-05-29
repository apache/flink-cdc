/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.base.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.EnvironmentInformation;

/** Utilities for environment information at runtime. */
public class EnvironmentUtils {

    private EnvironmentUtils() {}

    private static final VersionComparable FLINK_1_14 = VersionComparable.fromVersionString("1.14");

    private static final String ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH =
            "execution.checkpointing.checkpoints-after-tasks-finish.enabled";

    public static VersionComparable runtimeFlinkVersion() {
        return VersionComparable.fromVersionString(EnvironmentInformation.getVersion());
    }

    public static void requireCheckpointsAfterTasksFinished(Configuration configuration) {
        if (!supportCheckpointsAfterTasksFinished()) {
            throw new UnsupportedOperationException(
                    "To enabled checkpoints after tasks finished requires flink version greater than or equal to 1.14, current version is "
                            + runtimeFlinkVersion());
        }
        if (!enableCheckPointsAfterTaskFinishes(configuration)) {
            throw new IllegalArgumentException(
                    String.format(
                            "To enabled checkpoints after tasks finished requires '%s' set to true.",
                            ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH));
        }
    }

    public static boolean supportCheckpointsAfterTasksFinished() {
        return runtimeFlinkVersion().newerThanOrEqualTo(FLINK_1_14);
    }

    public static boolean enableCheckPointsAfterTaskFinishes(Configuration configuration) {
        return configuration.getBoolean(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
    }
}
