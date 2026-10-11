/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.dws.sink.v2;

import java.io.Serializable;

/** Fixed marker identifying checkpoints written by the official-client sink protocol. */
public final class DwsWriterState implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final DwsWriterState NATIVE_CLIENT_MARKER = new DwsWriterState();

    private DwsWriterState() {}

    public static DwsWriterState nativeClientMarker() {
        return NATIVE_CLIENT_MARKER;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DwsWriterState;
    }

    @Override
    public int hashCode() {
        return DwsWriterState.class.hashCode();
    }

    private Object readResolve() {
        return NATIVE_CLIENT_MARKER;
    }
}
