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

package com.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.runtime.util.EnvironmentInformation;

/** Utilities for environment information at runtime. */
public class EnvironmentUtils {

    private EnvironmentUtils() {}

    private static final VersionComparable FLINK_1_14 = VersionComparable.fromVersionString("1.14");

    public static VersionComparable runtimeFlinkVersion() {
        return VersionComparable.fromVersionString(EnvironmentInformation.getVersion());
    }

    public static boolean supportCheckpointsAfterTasksFinished() {
        return runtimeFlinkVersion().newerThanOrEqualTo(FLINK_1_14);
    }

    public static void checkSupportCheckpointsAfterTasksFinished(boolean closeIdleReaders) {
        if (closeIdleReaders && !supportCheckpointsAfterTasksFinished()) {
            throw new UnsupportedOperationException(
                    "The flink version is required to be greater than or equal to 1.14 when 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' is set to true. But the current version is "
                            + runtimeFlinkVersion());
        }
    }
}
