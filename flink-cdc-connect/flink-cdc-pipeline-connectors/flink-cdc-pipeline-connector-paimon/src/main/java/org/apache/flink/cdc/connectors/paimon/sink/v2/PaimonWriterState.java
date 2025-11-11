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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

/** The state of {@link PaimonWriter}. */
public class PaimonWriterState {

    public static final int VERSION = 0;

    /**
     * The commit user of {@link PaimonWriter}.
     *
     * <p>Note: Introduced from version 0.
     */
    private final String commitUser;

    private transient byte[] serializedBytesCache;

    public PaimonWriterState(String commitUser) {
        this.commitUser = commitUser;
    }

    public String getCommitUser() {
        return commitUser;
    }

    public byte[] getSerializedBytesCache() {
        return serializedBytesCache;
    }

    public void setSerializedBytesCache(byte[] serializedBytesCache) {
        this.serializedBytesCache = serializedBytesCache;
    }
}
