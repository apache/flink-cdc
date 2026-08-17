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

package org.apache.flink.core.execution;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;

import java.util.Optional;

/**
 * Compatibility adapter for Flink restore mode configuration.
 *
 * <p>This class is part of the multi-version compatibility layer that allows Flink CDC to work
 * across different Flink versions. It isolates version-specific Flink APIs for restore mode
 * configuration from the common Flink CDC code.
 *
 * <p>The adapter converts the Flink CDC {@code RestoreMode} to the corresponding Flink-specific
 * restore mode and configures it in the Flink {@code Configuration}.
 *
 * <p>Different Flink versions may use different restore mode types. For example, Flink 2.0 uses
 * {@code RecoveryClaimMode}, while older Flink versions may use {@code
 * SavepointConfigOptions.RestoreMode}.
 */
public final class RestoreModeAdapter {

    private RestoreModeAdapter() {}

    public static void setRestoreMode(Configuration configuration, RestoreMode restoreMode) {

        configuration.set(StateRecoveryOptions.RESTORE_MODE, convert(restoreMode));
    }

    public static void setSavepointIgnoreUnclaimedState(
            Configuration configuration, boolean ignoreUnclaimedState) {

        configuration.set(
                StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, ignoreUnclaimedState);
    }

    public static void setSavepointPath(Configuration configuration, String savepointPath) {

        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
    }

    public static RecoveryClaimMode getRestoreMode(Configuration configuration) {
        return configuration
                .getOptional(StateRecoveryOptions.RESTORE_MODE)
                .orElse(StateRecoveryOptions.RESTORE_MODE.defaultValue());
    }

    public static Optional<String> getSavepointPath(Configuration configuration) {
        return configuration.getOptional(StateRecoveryOptions.SAVEPOINT_PATH);
    }

    public static boolean getSavepointIgnoreUnclaimedState(Configuration configuration) {
        return configuration
                .getOptional(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE)
                .orElse(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.defaultValue());
    }

    private static RecoveryClaimMode convert(RestoreMode restoreMode) {
        switch (restoreMode) {
            case CLAIM:
                return RecoveryClaimMode.CLAIM;
            case NO_CLAIM:
                return RecoveryClaimMode.NO_CLAIM;
            case LEGACY:
                return RecoveryClaimMode.LEGACY;
            default:
                throw new IllegalArgumentException("Unsupported restore mode: " + restoreMode);
        }
    }
}
