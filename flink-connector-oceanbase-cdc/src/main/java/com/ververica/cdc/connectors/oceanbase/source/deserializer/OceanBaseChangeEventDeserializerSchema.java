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

package com.ververica.cdc.connectors.oceanbase.source.deserializer;

import org.apache.flink.annotation.PublicEvolving;

import com.oceanbase.oms.logmessage.LogMessage;

import java.io.Serializable;
import java.util.List;

/**
 * The deserializer interface describes how to turn the OceanBase change event into data types
 * (Java/Scala objects) that are processed by Flink.
 *
 * @param <T> The type created by the deserializer.
 */
@PublicEvolving
public interface OceanBaseChangeEventDeserializerSchema<T> extends Serializable {

    /**
     * Deserialize the change event from {@link LogMessage}.
     *
     * @param message Change event of {@link LogMessage} type.
     * @return Data after deserialization.
     */
    List<T> deserialize(LogMessage message);
}
