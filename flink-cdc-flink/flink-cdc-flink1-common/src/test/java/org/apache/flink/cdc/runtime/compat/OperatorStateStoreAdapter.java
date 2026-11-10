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

package org.apache.flink.cdc.runtime.compat;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.OperatorStateStore;

/**
 * Compatibility adapter for {@link OperatorStateStore} in Flink 1.20.
 *
 * <p>In Flink 1.x, OperatorStateStore interface does not have the v2 state descriptor methods.
 *
 * <p>This adapter provides a no-op interface that extends OperatorStateStore, allowing test code to
 * implement only the Flink 1.x methods.
 */
@Internal
public interface OperatorStateStoreAdapter extends OperatorStateStore {}
