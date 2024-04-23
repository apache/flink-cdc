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

package org.apache.flink.cdc.connectors.maxcompute.writer;

import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.impl.UpsertRecord;

import java.io.IOException;

/** the interface of all writer to write {@link ArrayRecord} to maxcompute. */
public interface MaxComputeWriter {

    SessionIdentifier getSessionIdentifier();

    UpsertRecord newElement();

    void write(UpsertRecord record) throws IOException;

    void delete(UpsertRecord record) throws IOException;

    void flush() throws IOException;

    String getId();
}
