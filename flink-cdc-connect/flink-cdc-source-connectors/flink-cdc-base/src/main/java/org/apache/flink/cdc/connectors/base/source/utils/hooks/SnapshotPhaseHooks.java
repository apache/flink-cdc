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

package org.apache.flink.cdc.connectors.base.source.utils.hooks;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A container class for hooks applied in the snapshot phase, including:
 *
 * <ul>
 *   <li>{@link #preHighWatermarkAction}: Hook to run before emitting high watermark, which is for
 *       testing whether stream events created within snapshot phase are backfilled correctly.
 *   <li>{@link #postHighWatermarkAction}: Hook to run after emitting high watermark, which is for
 *       testing actions handling stream events between snapshot splits.
 * </ul>
 */
public class SnapshotPhaseHooks implements Serializable {
    private static final long serialVersionUID = 1L;

    private SnapshotPhaseHook preLowWatermarkAction;
    private SnapshotPhaseHook postLowWatermarkAction;
    private SnapshotPhaseHook preHighWatermarkAction;
    private SnapshotPhaseHook postHighWatermarkAction;

    public void setPreHighWatermarkAction(SnapshotPhaseHook preHighWatermarkAction) {
        this.preHighWatermarkAction = preHighWatermarkAction;
    }

    public void setPostHighWatermarkAction(SnapshotPhaseHook postHighWatermarkAction) {
        this.postHighWatermarkAction = postHighWatermarkAction;
    }

    public void setPreLowWatermarkAction(SnapshotPhaseHook preLowWatermarkAction) {
        this.preLowWatermarkAction = preLowWatermarkAction;
    }

    public void setPostLowWatermarkAction(SnapshotPhaseHook postLowWatermarkAction) {
        this.postLowWatermarkAction = postLowWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPreHighWatermarkAction() {
        return preHighWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPostHighWatermarkAction() {
        return postHighWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPreLowWatermarkAction() {
        return preLowWatermarkAction;
    }

    @Nullable
    public SnapshotPhaseHook getPostLowWatermarkAction() {
        return postLowWatermarkAction;
    }

    public static SnapshotPhaseHooks empty() {
        return new SnapshotPhaseHooks();
    }
}
