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

package org.apache.flink.cdc.connectors.fluss.sink.v2;

import com.alibaba.fluss.client.Connection;

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializer to serialize the input record to a {@link FlussEvent} for {@link FlussSinkWriter}.
 *
 * @param <InputT> The type of the input record which comes from the upstream and will be
 *     transformed into a FlussEvent here.
 */
public interface FlussEventSerializer<InputT> extends Serializable {
    void open(Connection connection) throws IOException;

    FlussEvent serialize(InputT in) throws IOException;
}
