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

package org.apache.flink.cdc.connectors.values.source;

import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import java.util.Map;

/** A {@link SupportedMetadataColumn} for timestamp-type. */
public class TimestampTypeMetadataColumn implements SupportedMetadataColumn {

    @Override
    public String getName() {
        return "timestamp-type";
    }

    @Override
    public DataType getType() {
        return DataTypes.STRING();
    }

    @Override
    public Class<?> getJavaClass() {
        return String.class;
    }

    @Override
    public Object read(Map<String, String> metadata) {
        return metadata.getOrDefault(getName(), null);
    }
}
